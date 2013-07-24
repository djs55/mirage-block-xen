type t = {
  mutable notify_calls: int;
  mutable notify_skipped: int;
  mutable total_requests: int;
  mutable total_ok: int;
  mutable total_error: int;
}
let zero = {
  notify_calls = 0;
  notify_skipped = 0;
  total_requests = 0;
  total_ok = 0;
  total_error = 0;
}
let copy t = {
  notify_calls = t.notify_calls;
  notify_skipped = t.notify_skipped;
  total_requests = t.total_requests;
  total_ok = t.total_ok;
  total_error = t.total_error;
}
let to_string t = Printf.sprintf "{ Stats.notify_calls=%d; notify_skipped=%d; total_requests=%d; total_ok=%d; total_error=%d }"
  t.notify_calls t.notify_skipped t.total_requests t.total_ok t.total_error

