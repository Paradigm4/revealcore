test_that("Setup connection", {
  # Connect
  obj <- connect()
  # Should return object the inherits from Connection and R6
  expect_identical(class(obj), c("Connection", "R6"))
})

test_that("Test restoring serialized connection (with store_toke == TRUE)", {
  check_connection()

  # Connect (while storing obfuscated token)
  obj <- connect(store_token = TRUE)
  # Calling active attribute `aop_connection` _should_ revive connection and return a R6 object
  expect_type(unserialize(serialize(obj, NULL))$aop_connection, "environment")
})

test_that("Test failure to restore serialized connection (with store_toke == FALSE)", {
  check_connection()

  # Connect (while _not_ storing obfuscated token)
  obj <- connect()
  # Calling active attribute `aop_connection` can _not_ revive connection
  expect_error(unserialize(serialize(obj, NULL))$aop_connection, "Unable to reinitialize copied connection.")
})
