#' @importFrom scidb scidbconnect iquery
NULL

#' API Class for Querying and Computing in SciDB
#' @seealso \code{\link{connect}} for connection instructions.
connection <- setRefClass(
  "connection",
  fields = c(
    "host",
    "username",
    "port",
    "protocol",
    "db",
    "use_test_namespace",
    "scidb_ce",
    "scidb_version",
    "aop_connection",
    "roles"),
  methods = list()
)

#' Connect to scidb and return a connection object.
#'
#' @param username username of user to log in as
#' @param password password for the user.  If null, will be requested interactively.
#' @param host if NULL, attempt to set automatically from Apache config
#' @param port if NULL, attempt to set automatically from Apache config
#' @param protocol protocol type
#' @param results_size_limit Maximum size of any single result from a scidb query over this connection.  Query results larger than the specified size will error.
#' @param use_test_namespace if TRUE, use a single test namespace defined by the package schema, iff the package has such a namespace
#' @param multi_connection_environment if FALSE and the database connection is successful, store the connection object in a global environment variable.  This allows calling package functions without specifying a connection object.  Otherwise, remove connection object from global environment.
#'
#' @return a rg_connection object
#'
#' @export
connect = function(pkgEnv,
                   username = NULL,
                   password = NULL,
                   host = NULL,
                   port = NULL,
                   protocol = "https",
                   result_size_limit = 2*1048,
                   use_test_namespace=FALSE,
                   multi_connection_environment=F){
  # Setting the download limit size
  options(scidb.result_size_limit = result_size_limit)

  # SciDB connection and R API --

  if (is.null(username) & protocol != 'http') {
    cat("using HTTP protocol\n")
    protocol = 'http'
    unset_scidb_ee_flag = TRUE
  } else {
    unset_scidb_ee_flag = FALSE
  }

  if (!is.null(username) & protocol == 'http') {
    stop("if protocol is HTTP, cannot try authentication via HTTP")
  }

  con = NULL
  db = NULL
  if (is.null(username)) {
    protocol = 'http'
    if (is.null(host) & is.null(port)) {
      db = scidbconnect(protocol = protocol)
    } else {
      db = scidbconnect(host = host, port = port, protocol = protocol)
    }
  } else {
    # ask for password interactively if none supplied
    # https://github.com/Paradigm4/SciDBR/issues/154#issuecomment-327989402
    if (is.null(password)) {
      if (rstudioapi::isAvailable()) { # In RStudio,
        password = rstudioapi::askForPassword(paste0("Password for ", username, ":"))
      } else { # in base R
        password = getpwd()
      } # Rscripts and knitr not yet supported
    }

    if (is.null(password)) { # if still null password
      stop("Password cannot be null")
    }
    # Attempt 1.
    err1 = tryCatch({
      if (is.null(host)& is.null(port)) {
        # If user did not specify host and port, then formulate host URL from apache config
        path1 = '/etc/httpd-default/conf.d/default-ssl.conf'
        path2 = '/opt/rh/httpd24/root/etc/httpd/conf.d/25-default_ssl.conf'
        if (file.exists(path1) & !file.exists(path2)) {
          apache_conf_file = path1
          port = NULL
          hostname = NULL
        } else if (!file.exists(path1) & file.exists(path2)) {
          apache_conf_file = path2
          port = NULL
          hostname = NULL
        } else if (!file.exists(path1) & !file.exists(path2)) {
          hostname = 'localhost'
          port = 8083
        } else {
          cat("Cannot infer hostname from apache config. Need to supply hostname as parameter to connect\n")
          return(NULL)
        }
        if (is.null(hostname)) {
          hostname = tryCatch({
            system(paste0("grep ServerName ", apache_conf_file, " | awk '{print $2}'"),
                   intern = TRUE)
          },
          error = function(e) {
            cat("Could not infer hostname from apache conf\n")
            return(e)
          }
          )
          if (! "error" %in% class(hostname)) {
            hostname = paste0(hostname, '/shim/')
          } else {
            print(hostname)
            cat("Aborting connect()\n")
            return(NULL)
          }
        }
        cat("hostname was not provided. Connecting to", hostname, "\n")
        db = scidbconnect(host = hostname, username = username, password = password,
                          port = port, protocol = protocol)
      } else {
        # If user specified host and port, try user supplied parameters
        db = scidbconnect(host = host, username = username, password = password, port = port, protocol = protocol)
      }
    }, error = function(e) {return(e)}
    )

    if ("error" %in% class(err1)) {
      print(err1);
      db = NULL
    }
  }

  if(!is.null(db)){
    aop_connection = arrayop::db_connect(db=db, save_to_default_conn=!multi_connection_environment)
    if(aop_connection$scidb_version()$major >= 17 & username=='root'){username='scidbadmin'}
    con = connection(host = host,
                     username = username,
                     port = port,
                     protocol = protocol,
                     db = db,
                     use_test_namespace = (use_test_namespace & !is.null(pkgEnv$meta$L$package$test_namespace)),
                     scidb_ce = unset_scidb_ee_flag,
                     scidb_version = aop_connection$scidb_version(),
                     aop_connection = aop_connection,
                     roles=iquery(db,paste0("show_roles_for_user('",
                                            username,"')"),return=T)$role)
  }
  # Store a copy of connection object in package environment
  # Multi-session programs like Shiny, and the `rg_connect2` call need to explicitly delete this after rg_connect()
  if(!multi_connection_environment & !is.null(db)){
    pkgEnv$con = con
  } else {
    pkgEnv$con = NULL
  }
  return(con)
}
