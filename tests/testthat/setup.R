connect <- function(creds_file = "~/.scidb_auth_demo_user1", ...) {
  Connection$new(
    username = jsonlite::read_json(creds_file)[["user-name"]],
    password = jsonlite::read_json(creds_file)[["user-password"]],
    ...
  )
}

check_connection <- function() {
  if (!
  tryCatch(
    {
      connect()
      TRUE
    },
    error = function(...) FALSE
  )) {
    skip("Connection unavailable")
  }
}
