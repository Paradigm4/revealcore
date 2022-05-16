make_tmp_object <- function(conn) {
  return(conn$.private$.upload_df(data.frame(a = 1:10)))
}

test_that("Auto remove holds on to object", {
  check_connection()

  # Generate tmp array
  conn <- connect()
  obj <- make_tmp_object(conn)
  gc(full = TRUE)

  # We expect it to be there since we have a reference
  expect_identical(op_count(obj)$to_df()$count, 10)
})

test_that("Auto remove auto-removes object", {
  check_connection()

  # Generate tmp array
  conn <- connect()
  obj <- make_tmp_object(conn)
  gc()

  # We expect it to be there since we have a reference
  expect_identical(op_count(obj)$to_df()$count, 10)

  # Store afl string and check again (now with afl)
  array_name <- obj$to_afl()
  afl <- glue_me("op_count({array_name})")
  expect_identical(conn$query(afl)$count, 10)

  # Remove obj reference and run garbage collector
  rm(obj)
  gc(full = TRUE)

  # We now expect temp array to be gone.
  expect_error(conn$query(afl)$count, glue_me("Array 'public.{array_name}' does not exist."))
})
