#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2022 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

#' @importFrom R6 R6Class
#' @importFrom rlang new_data_mask enquo eval_tidy
#' @importFrom pryr address
#' @importFrom purrr reduce
#' @importFrom openssl sha256 aes_cbc_encrypt aes_cbc_decrypt
#' @importFrom glue glue identity_transformer
#' @importFrom digest digest
NULL

# Attribute names
ATTR_AUTO_REMOVE <- "revealconnect::autoremove"


# Connection ####

#' R6 Class representing database connection
#'
#' Provides standardized database connection API plus app-specific functions
#' @export
Connection <- R6Class("Connection",
  cloneable = FALSE,

  ## Public ####
  public = list(
    #' @description
    #' Initialize connection
    #' @param username user name
    #' @param password user password
    #' @param host host to connect to
    #' @param port port to connect to
    #' @param protocol connection protocol (e.g., "https")
    #' @param result_size_limit maximum size of database result
    #' @param use_test_namespace ?
    #' @param multi_connection_environment if FALSE, store connection in reveal env
    #' @param ... ignored
    #' @param disable_transactions If set, do not run `begin()`, `commit()`, `rollback()`
    #' @param store_token Store password info
    #' @param schema_path Path to schema file (defaults to package-level 'extdata/SCHEMA.yml')
    initialize = function(username = NULL,
                          password = NULL,
                          host = "127.0.0.1",
                          port = 8083,
                          protocol = "https",
                          result_size_limit = 2 * 1048,
                          use_test_namespace = FALSE,
                          multi_connection_environment = TRUE,
                          ...,
                          # Forced named args
                          disable_transactions = TRUE,
                          store_token = FALSE,
                          schema_path = system.file("extdata", "SCHEMA.yml", package = getPackageName())) {
      stopifnot("One or more unknown arguments" = missing(...))
      private$.last_known_address <- pryr::address(self)
      private$.rvlEnv <- new.env(parent = emptyenv())
      private$.rvlEnv$meta$L <- yaml::yaml.load_file(schema_path)
      private$connection <- connect(
        pkgEnv = self$rvlEnv,
        username = username,
        password = password,
        host = host,
        port = port,
        protocol = protocol,
        result_size_limit = result_size_limit,
        use_test_namespace = use_test_namespace,
        multi_connection_environment = multi_connection_environment
      )
      if (is.null(self$db)) {
        stop("Error connecting to db")
      }
      private$disable_transactions <- identical(TRUE, disable_transactions) ||
        identical(TRUE, get0(".disable_transactions"))
      private$result_size_limit <- as.integer(result_size_limit)
      private$multi_connection_environment <- identical(TRUE, multi_connection_environment)
      private$.tx_level <- 0
      private$.aop_obj_cache <- list()
      if (identical(store_token, TRUE)) {
        private$.set_token(password)
      }
    },

    #' @description
    #' Print connection object
    print = function() {
      cat(glue_me(
        "<{packageName()}::Connection ",
        "host={self$host}; port={self$port}; ",
        "username={self$username}; ",
        "roles=[{paste0(self$roles, collapse=',')}]>"
      ))
    },

    #' @description
    #' Run R code with aop_connection, db and roles set in environment
    #' @param code_block code to run
    with_connection = function(code_block) {
      ret <- eval_tidy(enquo(code_block),
        data = list(
          aop_connection = self$aop_connection,
          db = self$db,
          roles = self$roles
        )
      )
      return(invisible(ret))
    },

    #' @description
    #' Run R code in a single scidb transaction, with aop_connection, db and roles
    #' set in environment. Transactions are rolled back if errors occur at the database
    #' level, as well as in R code.
    #' @param code_block code to run
    transaction = function(code_block) {

      # Helper functions for transaction
      .scidb_begin <- function() {
        if (!private$disable_transactions) {
          if (nrow(self$query("current_txn()")) == 0) {
            self$aop_connection$execute("begin()")
          }
        }
      }
      .scidb_commit <- function() {
        if (!private$disable_transactions) {
          if (nrow(self$query("current_txn()")) > 0) {
            self$aop_connection$execute("commit()")
          }
        }
      }
      .scidb_rollback <- function(sleep = NULL) {
        if (!private$disable_transactions) {
          if (is.numeric(sleep) && sleep > 0) {
            Sys.sleep(sleep)
          }
          if (nrow(self$query("current_txn()")) > 0) {
            self$aop_connection$execute("rollback()")
          }
        }
      }

      # Start of transaction

      if (is_scidb_debug()) {
        message(glue_me(
          "==> TRANSACTION ENTER (level: {private$.tx_level})",
          "{ifelse(private$disable_transactions, ' [DISABLED]', '')}"
        ))
      }

      # Prevent nested scidb transactions by only beginning the outer-most level
      if (0 == private$.tx_level) {
        .scidb_begin()
      }
      private$.tx_level <- private$.tx_level + 1

      tryCatch(
        {

          # Run code inside a transaction, while injecting some db goodies into code's env
          ret <- eval_tidy(enquo(code_block),
            data = list(
              aop_connection = self$aop_connection,
              db = self$db,
              roles = self$roles
            )
          )
          if (1 == private$.tx_level) {
            .scidb_commit()
            if (is_scidb_debug()) {
              message(glue_me(
                "==> TRANSACTION COMMITTED (level: {private$.tx_level})",
                "{ifelse(private$disable_transactions, ' [DISABLED]', '')}"
              ))
            }
          }

          return(invisible(ret))
        },
        error = function(e) {
          # Only roll back outer-most level (i.e., we are the last tx)
          if (1 == private$.tx_level) {
            .scidb_rollback(sleep = 1)
            if (is_scidb_debug()) {
              message(glue_me(
                "==> TRANSACTION ROLLED BACK (level: {private$.tx_level})",
                "{ifelse(private$disable_transactions, ' [DISABLED]', '')}"
              ))
            }
          }
          stop(glue_me("Error in transaction; rolled back uncommitted changes:\n{e$message}"))
        },
        finally = {
          private$.tx_level <- private$.tx_level - 1
          if (is_scidb_debug()) {
            message(glue_me(
              "==> TRANSACTION EXIT (level: {private$.tx_level})",
              "{ifelse(private$disable_transactions, ' [DISABLED]', '')}"
            ))
          }
        }
      )
    },

    #' @description
    #' Run query (delegated to `self$aop_connection$query()`)
    #' @param afl_str Query string
    #' @return data.frame with query result
    query = function(afl_str) {
      return(self$aop_connection$query(afl_str))
    },

    #' @description
    #' Cached `afl_expr()`
    #' @param afl_str Query string
    #' @param key Key to store expression by
    #' @return data.frame with query result
    cached_afl_expr = function(afl_str, key = NULL) {
      stopifnot(is.character(afl_str), 1 == length(afl_str))
      if (!is.null(key)) {
        cache_name <- glue_me("AFL_EXPR: {digest::digest(key, 'sha256')}")
      } else {
        cache_name <- glue_me("AFL_EXPR: {digest::digest(afl_str, 'sha256')}")
      }
      if (private$aop_has_cache(cache_name)) {
        return(private$aop_from_cache(cache_name))
      }
      return(private$aop_cached(self$aop_connection$afl_expr(afl_str), cache_name))
    },

    #' @description
    #' Get entity, optionally filtered by id(s)
    #'
    #' @param entitynm Entity name
    #' @return arrayop query on entity, optionally semi_joined on id
    get_entity = function(entitynm) {
      stopifnot(is_entity(self$rvlEnv, entitynm))
      full_arrayname <- full_arrayname(
        pkgEnv = self$rvlEnv,
        entitynm = entitynm,
        con = self
      )
      cache_name <- glue_me("ENTITY: {entitynm}")
      if (!private$aop_has_cache(cache_name)) {
        scanned_entity <- scan_entity(
          pkgEnv = self$rvlEnv,
          entitynm = entitynm,
          con = self
        )
        if (full_arrayname == scanned_entity) {
          private$aop_cached(self$aop_connection$array(full_arrayname), cache_name)
        } else {
          private$aop_cached(self$aop_connection$afl_expr(scanned_entity), cache_name)
        }
      }
      return(private$aop_from_cache(cache_name))
    },

    #' @description
    #' Get entity names
    #'
    #' @param data_class data class to filter by
    #' @return character vector containing names of available entities
    get_entity_names = function(data_class = NULL) {
      stopifnot(is.null(data_class) || is.character(data_class))
      get_entity_names(self$rvlEnv, data_class = data_class)
    },

    #' @description
    #' Get entity id from name
    #'
    #' @param entitynm one or more entity names to obtain IDs for
    #' @return integer vector containing IDs of provided entities
    get_entity_id = function(entitynm) {
      stopifnot(is.character(entitynm))
      get_entity_id(self$rvlEnv, entitynm = entitynm)
    }

  ),

  ## Active ####
  active = list(
    # Expose private as .private
    #' @field .private Obtain reference to `private` for object
    .private = function(...) {
      private
    },

    # Read-only access to private revealcore environment
    #' @field rvlEnv Obtain _copy_ of revealcore environment
    rvlEnv = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      return(as.environment(as.list(private$.rvlEnv, all.names = TRUE)))
    },

    # Delegate connection attributes to revealcore connection
    #' @field username database user
    username = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$username
    },
    #' @field host database host
    host = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$host
    },
    #' @field port database port
    port = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$port
    },
    #' @field protocol database protocol
    protocol = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$protocol
    },
    #' @field db scidb database connection
    db = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$db
    },
    #' @field use_test_namespace ?
    use_test_namespace = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$use_test_namespace
    },
    #' @field scidb_ce community edition?
    scidb_ce = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$scidb_ce
    },
    #' @field scidb_version scidb version
    scidb_version = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$scidb_version
    },
    #' @field aop_connection array-op connection
    aop_connection = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$aop_connection
    },
    #' @field roles active database roles
    roles = function(...) {
      stopifnot("cannot assign read-only attribute" = missing(...))
      private$.init_copy()
      private$connection$roles
    }
  ),

  ## Protected/Private ####
  private = list(
    # Protected
    connection = NULL,
    disable_transactions = NULL,
    result_size_limit = NULL,
    multi_connection_environment = NULL,

    # Internal -- not to be accessed in derived classes
    .tx_level = NULL, # ensure we don't nest transactions
    .last_known_address = NULL,
    .p = NULL,
    .r = NULL,
    .aop_obj_cache = NULL,
    .rvlEnv = NULL, # revealcore environment

    # Decrypt token
    .get_token = function(...) {
      tryCatch(
        {
          rawToChar(aes_cbc_decrypt(private$.p, private$.r))
        },
        error = function(...) invisible()
      )
    },

    # Store encrypted token
    .set_token = function(password, ...) {
      tryCatch(
        {
          private$.r <- as.raw(sha256(charToRaw(random_alphanum(n = 32, prefix = ""))))
          private$.p <- aes_cbc_encrypt(charToRaw(password), private$.r)
        },
        error = function(...) invisible()
      )
    },

    # Re-initialize copies (as judged by address of self); requires .get_token() to
    # return not null.
    .init_copy = function() {

      # Return immediately if we are not a copy/clone
      if (private$.last_known_address == pryr::address(self)) {
        return(invisible(self))
      }

      ###
      ##  We are a copy with respect to when we last checked
      ###

      # Update our address
      private$.last_known_address <- pryr::address(self)

      # Decrypt stored token
      token <- private$.get_token()
      if (is.null(token)) {
        stop("Unable to reinitialize copied connection.")
      }

      # Setup new connection
      new_connection <- connect(
        pkgEnv = self$rvlEnv,
        username = self$username,
        password = token,
        host = self$host,
        port = self$port,
        protocol = self$protocol,
        result_size_limit = private$result_size_limit,
        use_test_namespace = self$use_test_namespace,
        multi_connection_environment = private$multi_connection_environment
      )
      if (!is.null(new_connection)) {
        # Connect apparently succeeded
        private$connection <- new_connection

        # Clear cache
        private$.aop_obj_cache <- list()

        # Reset transaction level
        private$.tx_level <- 0

        # Re-encrypt token
        private$.set_token(token)
      } else {
        stop("Unable to reinitialize copied connection.")
      }

      return(invisible(self))
    },

    # TODO: suggest changes to arrayop
    # NOTE: probably only works R>4.0

    # Upload df to temp array
    .upload_df = function(df, name = NULL, auto_remove = T) {
      stopifnot(inherits(df, "data.frame"))
      stopifnot(is.null(name) || is.character(name) && 1 == length(name))
      auto_remove <- identical(TRUE, auto_remove)
      if (!is.null(name)) {
        ret <-
          self$aop_connection$upload_df(as.data.frame(df),
            .temp = T, .gc = F, .use_aio_input = T, name = name
          )
      } else {
        ret <-
          self$aop_connection$upload_df(as.data.frame(df),
            .temp = T, .gc = F, .use_aio_input = T
          )
      }
      if (auto_remove) {
        return(.auto_remove(ret))
      }
      return(ret)
    },

    ###
    ## Caching API
    ###

    # Does `name` exist in cache?
    aop_has_cache = function(name) {
      stopifnot(is.null(name) || is.character(name) && 1 == length(name))
      private$.init_copy()
      return(name %in% names(private$.aop_obj_cache))
    },

    # Get object cached by `name`
    aop_from_cache = function(name) {
      stopifnot(is.null(name) || is.character(name) && 1 == length(name))
      private$.init_copy()
      return(private$.aop_obj_cache[[name]])
    },

    # Cache object by `name` (or `obj$to_afl()` if `NULL`) and return
    # cached object
    aop_cached = function(obj, name = NULL) {
      stopifnot(inherits(obj, "ArrayOpBase"))
      stopifnot(is.null(name) || is.character(name) && 1 == length(name))
      private$.init_copy()
      if (is.null(name)) {
        name <- obj$to_afl()
      }
      if (!(name %in% names(private$.aop_obj_cache))) {
        private$.aop_obj_cache[[name]] <- obj
      }
      return(private$.aop_obj_cache[[name]])
    }
  )
)

# .AutoRemove ####

#' Wrapper to auto-remove temporary persistent arrays
#' @description R6 Class to track arrayop `ArrayOpBase` object and auto-remove when
#'              it is garbage collected
.AutoRemove <- R6::R6Class(".AutoRemove",
  cloneable = FALSE,

  ## Public ####
  public = list(
    #' @field array Reference to `ArrayOpBase` object
    array = NULL,
    #' @description Initialize new `.AutoRemove` with an `ArrayOpBase` object
    #' @param array `ArrayOpBase` object
    #' @return new `.AutoRemove` object
    initialize = function(array) {
      stopifnot(inherits(array, "ArrayOpBase"))
      if (is_scidb_debug()) {
        message(glue_me("==> .AutoRemove - init: array: {array$to_afl()}"))
      }
      self$array <- array
    },
    #' @description Remove `ArrayOpBase` object when garbage collected
    finalize = function() {
      if (is_scidb_debug()) {
        message(glue_me("==> .AutoRemove - finalize: array: {self$array$to_afl()}"))
      }
      tryCatch(
        {
          self$array$remove_array()
        },
        error = function(...) {}
      )
    }
  )
)

# Helper functions ####

#' Wrap array in `.AutoRemove` for cleanup during garbage collection
#'
#' This can be used to alleviate gc() issues in arrayop. Generates a new
#' `.AutoRemove` object and attaches it to `array` as an attribute.
#' @param array array to auto remove
#' @return array with .AutoRemove ref added as attribute
.auto_remove <- function(array) {
  stopifnot(inherits(array, "ArrayOpBase"))
  attr(array, ATTR_AUTO_REMOVE) <- .AutoRemove$new(array)
  return(array)
}

#' Generate nested filters from a list of expressions and an arrayop object
#'
#' Given an object obj and a list of expressions `list(a == 1, x > 0, etc.)`, generate
#' calls `obj$filter(a == 1)$filter(x > 0)$filter(....)`.
#'
#' @param array arrayop to filter
#' @param expressions sequence of expressions (i.e. unevaluated expressions such as
#'                    returned by str2lang)
#' @param case_sensitive inverted and passed on to .ignore_case of filter()
#' @param ... extra arguments passed to _all_ nested filter calls
#' @return Result of `array$filter(....)$filter(....)....`
#' @export
filter_reducer <- function(array, expressions, case_sensitive = TRUE, ...) {
  # Check/normalize input
  stopifnot(inherits(array, "ArrayOpBase"))
  stopifnot(is.list(expressions))
  case_sensitive <- !!case_sensitive[1]
  # Reduce filter expressions
  purrr::reduce(expressions, function(a, b, ...) {
    do.call(a$filter, list(b, ...))
  }, .ignore_case = !identical(case_sensitive, TRUE), .init = array, ...)
}

#' Map R to scidb data type
#' @export
data_type_mapping_r_to_scidb <- setNames(
  c("int64", "string", "numeric", "logical"),
  c("integer", "character", "float", "bool")
)

#' Map scidb to R data type
#' @export
data_type_mapping_scidb_to_r <- setNames(
  c(names(data_type_mapping_r_to_scidb), "integer"),
  c(as.character(data_type_mapping_r_to_scidb), "int32")
)

#' Random alias
#'
#' @param name prefix
.random_alias <- function(name) {
  stopifnot(is.character(name), 1 == length(name))
  random_alphanum(prefix = sprintf("_%s", name), suffix = "", n = 3)
}

#' Random left alias
#'
#' @param name optional prefix
#' @export
left_alias <- function(name = NULL) {
  .random_alias(ifelse(is.null(name), "L", sprintf("%s_", name)))
}

#' Random right alias
#'
#' @param name optional prefix
#' @export
right_alias <- function(name = NULL) {
  .random_alias(ifelse(is.null(name), "R", sprintf("%s_", name)))
}

#' Generate random alpha-numeric string
#'
#' @param prefix prefix for random string
#' @param suffix suffix for random string
#' @param n length of string (excluding prefix and suffix)
#' @export
random_alphanum <- function(prefix = "_", suffix = "", n = 3L) {
  sprintf(
    "%s%s%s", prefix,
    rawToChar(as.raw(sample(c(48:57, 65:90, 97:122), n, replace = TRUE))),
    suffix
  )
}

#' Generate grouped_aggregate() expression
#'
#' Generate afl expression for `grouped_aggregate(...)`
#' and `redimension(grouped_aggregate(...), ...)`
#'
#' @param aop arrayop object
#' @param ... fields to group on
#' @param fun aggregate function
#' @param schema optional schema string to redimension into
#' @return aop object
#' @export
grouped_aggregate <- function(aop, ..., fun = "count(*)", schema = NULL) {
  stopifnot(inherits(aop, "ArrayOpBase"))
  stopifnot(!missing(...), sapply(list(...), is.character), sapply(list(...), length) == 1)
  stopifnot(is.character(fun) && 1 == length(fun))
  stopifnot(is.null(schema) || is.character(schema) && 1 == length(schema))
  args <- paste(..., sep = ", ")
  ret <- glue_me("grouped_aggregate({aop$to_afl()}, {fun}, {args})")
  if (!is.null(schema)) {
    stopifnot("Invalid schema string" = grepl("^<.*> *\\[.*\\]$", schema))
    ret <- glue_me("redimension({ret}, {schema})")
  }
  # TODO: propose conn be exposed publicly in arrayop
  return(aop$.private$conn$afl_expr(ret))
}

#' Generate op_count() expression
#'
#' Generate afl expression for `op_count(...)`
#'
#' @param aop arrayop object
#' @return aop object
#' @export
op_count <- function(aop) {
  stopifnot(inherits(aop, "ArrayOpBase"))
  ret <- glue_me("op_count({aop$to_afl()})")
  # TODO: propose conn be exposed publicly in arrayop
  return(aop$.private$conn$afl_expr(ret))
}

#' Remove versions of array after checking existence of versions
#'
#' @param aop arraop array to remove versions on
#' @return reference to `aop`
#' @export
remove_versions <- function(aop) {
  stopifnot(inherits(aop, "ArrayOpBase"))
  if (nrow(aop$list_versions()) > 1) {
    aop$remove_versions()
  }
  return(invisible(aop))
}

#' Check for `options(scidb.debug=TRUE)`
#' @export
is_scidb_debug <- function() {
  debug <- unlist(options("scidb.debug"))
  return(!is.null(debug) && !!debug)
}

#' De-vectorized, NULL-immune `glue()`
#'
#' @param ... passed on to `glue::glue()`
#' @param none string value to use for NA and NULL
#' @param .envir execution environment
#' @export
glue_me <- function(..., none = "(none)", .envir = parent.frame()) {
  transformer <- function(text, ...) {
    transformed <- glue::identity_transformer(text, ...)
    if (is.null(transformed) || 0 == length(transformed)) {
      transformed <- sprintf("%s", toString(none))
    }
    toString(transformed)
  }
  glue::glue(..., .envir = .envir, .transformer = transformer)
}
