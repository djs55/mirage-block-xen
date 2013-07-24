(*
 * Copyright (c) 2011 Anil Madhavapeddy <anil@recoil.org>
 * Copyright (c) 2012 Citrix Systems Inc
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt
open Printf
open OS
open Blkproto
open Gnt

type features = {
  barrier: bool;
  removable: bool;
  sector_size: int64; (* stored as int64 for convenient division *)
  sectors: int64;
  readwrite: bool;
}

type transport = {
  backend_id: int;
  backend: string;
  ring: (Res.t,int64) Ring.Rpc.Front.t;
  client: (Res.t,int64) Lwt_ring.Front.t;
  gnts: Gnt.gntref list;
  evtchn: Eventchn.t;
  features: features;
  stats: Blkstats.t;
  wakers: (int64, Res.t Lwt.u) Hashtbl.t; (* id * wakener *)
  pending_requests: Req.t Lwt_sequence.t;
  mutable num_pending_requests: int;
  transmitter_c: unit Lwt_condition.t;
}

type t = {
  vdev: int;
  mutable t: transport
}

type id = string
exception IO_error of string

let h = Eventchn.init ()

let rec diagnostic_thread ?previous t =
  lwt () = OS.Time.sleep 10. in
  let this = Blkstats.copy t.stats in
  printf "%s\n%!" (Blkstats.to_string this);
  if previous = Some this then begin
    printf "<-- nothing changed in 10 seconds\n%!";
    printf "%s\n%!" (Ring.Rpc.Front.to_string t.ring);
    printf "triggering a notify\n%!";
    Eventchn.notify h t.evtchn 
  end;
  diagnostic_thread ~previous:this t

(** Set of active block devices *)
let devices : (id, t) Hashtbl.t = Hashtbl.create 1

(* Allocate a ring, given the vdev and backend domid *)
let alloc ~order (num,domid) =
  let name = sprintf "Blkif.%d" num in
  let idx_size = Req.Proto_64.total_size in (* bigger than res *)
  let buf = Io_page.get_order order in

  let pages = Io_page.to_pages buf in
  lwt gnts = Gntshr.get_n (List.length pages) in
  List.iter (fun (gnt, page) -> Gntshr.grant_access ~domid ~writeable:true gnt page) (List.combine gnts pages);

  let sring = Ring.Rpc.of_buf ~buf:(Io_page.to_cstruct buf) ~idx_size ~name in
  printf "Blkfront %s\n%!" (Ring.Rpc.to_summary_string sring);
  let fring = Ring.Rpc.Front.init ~sring in
  let client = Lwt_ring.Front.init Int64.to_string fring in
  return (gnts, fring, client)

let poll t =
  Ring.Rpc.Front.ack_responses t.ring (fun slot ->
    let id, resp = Res.read_response slot in
    try
       let u = Hashtbl.find t.wakers id in
       Hashtbl.remove t.wakers id;
       Lwt.wakeup_later u resp;
     with Not_found ->
       let string_of_id = Int64.to_string in
       printf "Block RX: ack (id = %s) wakener not found\n" (string_of_id id);
       printf "    valid ids = [ %s ]\n%!" (String.concat "; " (List.map string_of_id (Hashtbl.fold (fun k _ acc -> k :: acc) t.wakers [])));
    )

(* Thread to poll for responses and activate wakeners *)
let receiver_thread t =
  let rec loop_forever event =
    lwt event = Activations.after t.evtchn event in
(*  Printf.printf "Activations.wait %d\n%!" (Eventchn.to_int t.evtchn); *)
(*  Printf.printf "polling ring\n%!"; *)
    let () = poll t in
    (* There might now be space in the ring: *)
    if Ring.Rpc.Front.get_free_requests t.ring > 0
    && t.num_pending_requests > 0 
    then Lwt_condition.signal t.transmitter_c ();
    loop_forever event in
  loop_forever Activations.program_start

let transmitter_thread t =
  let rec loop_forever () =
    (* We must wait if there are no pending requests (ie no work to do)
       or if there are no free slots in the ring. *)
    lwt () =
      while_lwt (t.num_pending_requests = 0)
                || (Ring.Rpc.Front.get_free_requests t.ring = 0) do
      Lwt_condition.wait t.transmitter_c done in
    (* Take as many available requests that will fit on the ring *)
    for i = 1 to min t.num_pending_requests (Ring.Rpc.Front.get_free_requests t.ring) do
      let req = Lwt_sequence.take_l t.pending_requests in
      t.num_pending_requests <- t.num_pending_requests - 1;
      assert (Ring.Rpc.Front.get_free_requests t.ring > 0);
      let slot_id = Ring.Rpc.Front.next_req_id t.ring in
      let slot = Ring.Rpc.Front.slot t.ring slot_id in
      let (request_id: int64) = Req.Proto_64.write_request req slot in
let notify = Ring.Rpc.Front.push_requests_and_check_notify t.ring in
    if notify
    then t.stats.Blkstats.notify_calls <- t.stats.Blkstats.notify_calls + 1
    else t.stats.Blkstats.notify_skipped <- t.stats.Blkstats.notify_skipped + 1;
    if notify
    then Eventchn.notify h t.evtchn;
      ()
    done;
    (* Push the batch and consider sending a notify *)
    loop_forever () in
  loop_forever ()

let rpc t req =
  let th, u = Lwt.task () in
  Hashtbl.add t.t.wakers req.Req.id u;

  Lwt.on_cancel th (fun () -> Hashtbl.remove t.t.wakers req.Req.id);

  let (_: 'a Lwt_sequence.node) = Lwt_sequence.add_r req t.t.pending_requests in
  t.t.num_pending_requests <- t.t.num_pending_requests + 1;
  Lwt_condition.signal t.t.transmitter_c ();
  th

(* Given a VBD ID and a backend domid, construct a blkfront record *)
let plug (id:id) =
  lwt vdev = try return (int_of_string id)
    with _ -> fail (Failure "invalid vdev") in
  printf "Blkfront.create; vdev=%d\n%!" vdev;
  let node = sprintf "device/vbd/%d/%s" vdev in

  lwt backend_id = Xs.(immediate (fun h -> read h (node "backend-id"))) in
  lwt backend_id = try_lwt return (int_of_string backend_id)
    with _ -> fail (Failure "invalid backend_id") in
  lwt backend = Xs.(immediate (fun h -> read h (node "backend"))) in

  let backend_read fn default k =
    let backend = sprintf "%s/%s" backend in
      try_lwt
        lwt s = Xs.(immediate (fun h -> read h (backend k))) in
        return (fn s)
      with exn -> return default in

  (* The backend can advertise a multi-page ring: *)
  lwt backend_max_ring_page_order = backend_read int_of_string 0 RingInfo._max_ring_page_order in
  if backend_max_ring_page_order = 0
  then printf "Blkback can only use a single-page ring\n%!"
  else printf "Blkback advertises multi-page ring (size 2 ** %d pages)\n%!" backend_max_ring_page_order;

  let our_max_ring_page_order = 2 in (* 4 pages *)
  let ring_page_order = min our_max_ring_page_order backend_max_ring_page_order in
  printf "Negotiated a %s\n%!" (if ring_page_order = 0 then "singe-page ring" else sprintf "multi-page ring (size 2 ** %d pages)" ring_page_order);

  lwt (gnts, ring, client) = alloc ~order:ring_page_order (vdev,backend_id) in
  let evtchn = Eventchn.bind_unbound_port h backend_id in
  let port = Eventchn.to_int evtchn in
  let ring_info = { RingInfo.order = ring_page_order; refs = gnts; event_channel = port; protocol = Protocol.X86_64 } in
  let pairs =
    State.(to_assoc_list Connected)
    @ (RingInfo.to_assoc_list ring_info) in
  lwt () = Xs.(transaction (fun h ->
    Lwt_list.iter_s (fun (k, v) -> write h (node k) v) pairs
  )) in
  lwt () = Xs.(wait (fun h ->
    lwt state = read h (sprintf "%s/state" backend) in
    if Device_state.(of_string state = Connected) then return () else fail Xs_protocol.Eagain
  )) in
  (* Read backend features *)
  lwt features =
    lwt state = backend_read (Device_state.of_string) Device_state.Unknown "state" in
    printf "state=%s\n%!" (Device_state.prettyprint state);
    lwt barrier = backend_read ((=) "1") false "feature-barrier" in
    lwt removable = backend_read ((=) "1") false "removable" in
    lwt sectors = backend_read Int64.of_string (-1L) "sectors" in
    lwt sector_size = backend_read Int64.of_string 0L "sector-size" in
    lwt readwrite = backend_read (fun x -> x = "w") false "mode" in
    return { barrier; removable; sector_size; sectors; readwrite }
  in
  printf "Blkfront features: barrier=%b removable=%b sector_size=%Lu sectors=%Lu\n%!" 
    features.barrier features.removable features.sector_size features.sectors;
(*  Eventchn.unmask h evtchn; *)
  let stats = Blkstats.zero in
  let wakers = Hashtbl.create 32 in
  let pending_requests = Lwt_sequence.create () in
  let num_pending_requests = 0 in
  let transmitter_c = Lwt_condition.create () in
  let t = { backend_id; backend; ring; client; gnts; evtchn; features; stats; pending_requests; num_pending_requests; transmitter_c; wakers } in
  let _ = receiver_thread t in
  let _ = transmitter_thread t in
  let _ = diagnostic_thread t in
  return t

(* Unplug shouldn't block, although the Xen one might need to due
   to Xenstore? XXX *)
let unplug id =
  Console.log (sprintf "Blkif.unplug %s: not implemented yet" id);
  ()

(** Return a list of valid VBDs *)
let enumerate () =
  try_lwt
    Xs.(immediate (fun h -> directory h "device/vbd"))
  with
    | Xs_protocol.Enoent _ ->
      return []
    | e ->
      Console.log (sprintf "Blkif.enumerate caught exception: %s" (Printexc.to_string e));
      return []

(* Write a single page to disk.
   Offset is the sector number, which must be sector-aligned
   Page must be an Io_page *)
let rec write_page t offset page =
  let sector = Int64.div offset t.t.features.sector_size in
  if not t.t.features.readwrite
  then fail (IO_error "read-only")
  else 
    try_lwt
      Gntshr.with_ref
      (fun r ->
        Gntshr.with_grant ~domid:t.t.backend_id ~writeable:true r page
          (fun () ->
            let gref = Int32.of_int r in
            let id = Int64.of_int32 gref in
            let segs =[| { Req.gref; first_sector=0; last_sector=7 } |] in
            let req = Req.({op=Some Req.Write; handle=t.vdev; id; sector; segs}) in
            lwt res = rpc t req in
            let open Res in
            Res.(match res.st with
            | Some Error ->
              printf "IO error\n%!";
              fail (IO_error "write")
            | Some Not_supported ->
              printf "unsupported\n%!";
              fail (IO_error "unsupported")
            | None ->
              printf "unknown\n%!";
              fail (IO_error "unknown error")
            | Some OK -> return ())
          )
      )
       with 
	 | Lwt_ring.Shutdown -> write_page t offset page
	 | exn -> fail exn 

module Single_request = struct
  (** A large request must be broken down into a series of smaller page-aligned requests: *)
  type t = {
    start_sector: int64; (* page-aligned sector to start reading from *)
    start_offset: int;   (* sector offset into the page of our data *)
    end_sector: int64;   (* last page-aligned sector to read *)
    end_offset: int;     (* sector offset into the page of our data *)
  }

  (** Number of pages required to issue this request *)
  let npages_of t = Int64.(to_int (div (sub t.end_sector t.start_sector) 8L))

  let to_string t =
    sprintf "(%Lu, %u) -> (%Lu, %u)" t.start_sector t.start_offset t.end_sector t.end_offset

  (* Transforms a large read of [num_sectors] starting at [sector] into a Lwt_stream
     of single_requests, where each request will fit on the ring. *)
  let stream_of sector num_sectors =
    let from (sector, num_sectors) =
      assert (sector >= 0L);
      assert (num_sectors > 0L);
      (* Round down the starting sector in order to get a page aligned sector *)
      let start_sector = Int64.(mul 8L (div sector 8L)) in
      let start_offset = Int64.(to_int (sub sector start_sector)) in
      (* Round up the ending sector to the page boundary *)
      let end_sector = Int64.(mul 8L (div (add (add sector num_sectors) 7L) 8L)) in
      (* Calculate number of sectors needed *)
      let total_sectors_needed = Int64.(sub end_sector start_sector) in
      (* Maximum of 11 segments per request; 1 page (8 sectors) per segment so: *)
      let total_sectors_possible = min 88L total_sectors_needed in
      let possible_end_sector = Int64.add start_sector total_sectors_possible in
      let end_offset = min 7 (Int64.(to_int (sub 7L (sub possible_end_sector (add sector num_sectors))))) in

      let first = { start_sector; start_offset; end_sector = possible_end_sector; end_offset } in
      if total_sectors_possible < total_sectors_needed
      then
        let num_sectors = Int64.(sub num_sectors (sub total_sectors_possible (of_int start_offset))) in
        first, Some ((Int64.add start_sector total_sectors_possible), num_sectors)
      else
        first, None in
    let state = ref (Some (sector, num_sectors)) in
    Lwt_stream.from
      (fun () ->
        match !state with
        | None -> return None
        | Some x ->
          let item, state' = from x in
          state := state';
          return (Some item)
      )
end

exception Timeout

(* Issues a single request to read from [start_sector + start_offset] to [end_sector - end_offset]
   where: [start_sector] and [end_sector] are page-aligned; and the total number of pages will fit
   in a single request. *)
let read_single_request t r =
  let open Single_request in
  let len = npages_of r in
  if len > 11 then
    fail (Failure (sprintf "len > 11 %s" (Single_request.to_string r)))
  else 
    let pages = Io_page.(to_pages (get len)) in
    let rec single_attempt () =
      try_lwt
	Gntshr.with_refs len
          (fun rs ->
            Gntshr.with_grants ~domid:t.t.backend_id ~writeable:true rs pages
	      (fun () ->
		let segs = Array.mapi
		  (fun i rf ->
		    let first_sector = match i with
		      |0 -> r.start_offset
		      |_ -> 0 in
		    let last_sector = match i with
		      |n when n == len-1 -> r.end_offset
		      |_ -> 7 in
		    let gref = Int32.of_int rf in
		    { Req.gref; first_sector; last_sector }
		  ) (Array.of_list rs) in
		let id = Int64.of_int (List.hd rs) in
		let req = Req.({ op=Some Read; handle=t.vdev; id; sector=r.start_sector; segs }) in
		lwt res = rpc t req in
(*
                let timeout =
                  lwt () = OS.Time.sleep 30. in
                  printf "timeout\n%!";
                  fail Timeout in
                lwt res = Lwt.pick [ res; timeout ] in
*)
		let open Res in
		match res.st with
		  | Some Error ->
                    printf "read error\n%!";
                    t.t.stats.Blkstats.total_error <- t.t.stats.Blkstats.total_error + 1;
                    fail (IO_error "read")
		  | Some Not_supported ->
                    printf "unsupported\n%!";
                    t.t.stats.Blkstats.total_error <- t.t.stats.Blkstats.total_error + 1;
                    fail (IO_error "unsupported")
		  | None ->
                    printf "unknown\n%!";
                    t.t.stats.Blkstats.total_error <- t.t.stats.Blkstats.total_error + 1;
                    fail (IO_error "unknown error")
		  | Some OK ->
                    t.t.stats.Blkstats.total_ok <- t.t.stats.Blkstats.total_ok + 1;
		    (* Get the pages, and convert them into Istring views *)
		    return (Lwt_stream.of_list (List.rev (snd (List.fold_left
		      (fun (i, acc) page ->
			let start_offset = match i with
			  |0 -> r.start_offset * 512
			  |_ -> 0 in
			let end_offset = match i with
			  |n when n = len-1 -> (r.end_offset + 1) * 512
			  |_ -> 4096 in
			let bytes = end_offset - start_offset in
			let subpage = Cstruct.sub (Io_page.to_cstruct page) start_offset bytes in
			i + 1, subpage :: acc
		      ) (0, []) pages
		    ))))
	      )
	  )
      with | Lwt_ring.Shutdown -> single_attempt ()
           | exn -> fail exn in
      single_attempt ()

(* Reads [num_sectors] starting at [sector], returning a stream of Io_page.ts *)
let read_512 t sector num_sectors =
  let requests = Single_request.stream_of sector num_sectors in
  Lwt_stream.(concat (map_s (read_single_request t) requests))

let resume t =
  let vdev = sprintf "%d" t.vdev in
  lwt transport = plug vdev in
  let old_t = t.t in
  t.t <- transport;
  Lwt_ring.Front.shutdown old_t.client;
  return ()

let resume () =
  let devs = Hashtbl.fold (fun k v acc -> (k,v)::acc) devices [] in
  Lwt_list.iter_p (fun (k,v) -> resume v) devs

let create ~id : Devices.blkif Lwt.t =
  printf "Xen.Blkif: create %s\n%!" id;
  lwt trans = plug id in
  let dev = { vdev = int_of_string id;
 	      t = trans } in
  Hashtbl.add devices id dev;
  printf "Xen.Blkif: success\n%!";
  return (object
    method id = id
    method read_512 = read_512 dev
    method write_page = write_page dev
    method sector_size = 4096
    method size = Int64.mul dev.t.features.sectors dev.t.features.sector_size
    method readwrite = dev.t.features.readwrite
    method ppname = sprintf "Xen.blkif:%s" id
    method destroy = unplug id
  end)

(* Register Xen.Blkif provider with the device manager *)
let register () =
  printf "Xen.Blkfront.register\n%!";
  let plug_mvar = Lwt_mvar.create_empty () in
  let unplug_mvar = Lwt_mvar.create_empty () in
  let provider = object(self)
     method id = "Xen.Blkif"
     method plug = plug_mvar 
     method unplug = unplug_mvar
     method create ~deps ~cfg id =
	  (* no cfg required: we will check xenstore instead *)
      lwt blkif = create ~id in
      let entry = Devices.({
        provider=self; 
        id=self#id; 
        depends=[];
        node=Blkif blkif }) in
      return entry
  end in
  Devices.new_provider provider;
  (* Iterate over the plugged in VBDs and plug them in *)
  lwt ids = enumerate () in
  Console.log (sprintf "Blkif.enumerate found ids [ %s ]" (String.concat "; " ids));
    let vbds = List.map (fun id ->
      printf "found VBD with id: %s\n%!" id;
      { Devices.p_dep_ids = []; p_cfg = []; p_id = id }
    ) ids in
  Lwt_list.iter_s (Lwt_mvar.put plug_mvar) vbds

let _ =
  printf "Blkif: add resume hook\n%!";
  Sched.add_resume_hook resume
