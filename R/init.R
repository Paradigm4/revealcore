# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2017 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT

#' Initialize or reinitialize arrays, roles, and namespaces associated with a REVEAL package
#'
#' The function requires that you are connected to SciDB as an user with admin privileges
#'
#' @param con a connection object
#' @param pkgEnv a REVEAL package environment object
#' @param init_arrays if TRUE, delete and initialize array.  Use with caution.
#' @param init_namespaces if TRUE, initialize namespaces.  Run only on first-time database initialization
#' @param init_roles if TRUE, initialize roles.  Run only on first-time database initialization
#' @param arrays_to_init (optional) if init_arrays is TRUE, list of arrays to delete and initialize.  if NULL (default), delete and initialize all arrays
#' @param namespaces_to_init (optional) if init_namespaces is TRUE, list of namespaces to initialize.  if NULL (default), initialize all namespaces.
#' @param roles_to_init (optional) if init_roles is TRUE, list of roles to initialize.  if NULL (default), initialize all roles.
#' @param force if TRUE, do not ask for confirmation prior to deletion / initialization
#' @param silent if TRUE, suppress messaging of individual namespace / role/ array deletion & initialization
#' @param allow_array_reinit if TRUE, allow deletion of arrays that are already initialized when init_arrays is TRUE.  if FALSE (default), initialize only arrays that do not yet exist.
#'
#' @return NULL
#'
#' @export
init_db = function(pkgEnv,
                   con,
                   init_arrays = TRUE,
                   init_namespaces = FALSE,
                   init_roles = FALSE,
                   arrays_to_init = NULL,
                   namespaces_to_init = NULL,
                   roles_to_init = NULL,
                   force = FALSE,
                   silent = FALSE,
                   allow_array_reinit = FALSE){
  if(!check_user_admin_status(con)){
    stop("Run only with admin priviledges.")
  }

  if(init_namespaces) {
    init_namespaces(pkgEnv = pkgEnv,
                    con = con,
                    namespaces_to_init = namespaces_to_init,
                    force = force,
                    silent = silent)
  }
  if(init_roles){
    init_roles(pkgEnv = pkgEnv,
               con = con,
               roles_to_init = roles_to_init,
               force = force,
               silent = silent)
  }
  if(init_arrays){
    init_arrays(pkgEnv = pkgEnv,
                con = con,
                arrays_to_init = arrays_to_init,
                force = force,
                silent = silent,
                allow_array_reinit = allow_array_reinit)
  }
}

#' @export
init_permissions_array = function(pkgEnv, con) {
  db = con$db

  permissions_arr = PERMISSIONS_ARRAY(pkgEnv, con)
  if(is_entity(strip_namespace(PERMISSIONS_ARRAY(pkgEnv, con)))){
    stop("Permissions array configured as entity.  Manage with init_arrays.")
  }
  if(is.null(permissions_arr)){
    stop("Permissions array not configured.")
  }

  cat("Try deleting permissions array (if exists)\n")
  tryCatch({
    iquery(db, paste0("remove(", permissions_arr, ")"))
  },
  error = function(e) {
    cat("====Failed to remove permissions array\n")
  })

  cat("Create permissions array\n")
  starting_dimension="0"
  if(con$scidb_version$major>=21 |
      (con$scidb_version$major==19 &&
         con$scidb_version$minor==11 &&
         con$scidb_version$patch>=8)
  ){
    starting_dimension="-1"
  }
  tryCatch({
    iquery(db, paste0("create array ", permissions_arr,
                      " <access:bool> [user_id=",starting_dimension,":*; ",
                      pkgEnv$meta$L$package$secure_dimension,"=0:*]"))
    iquery(db, paste0("store(", permissions_arr, ", ", permissions_arr, ")"))
  },
  error = function(e) {
    cat("====Failed to create permissions array\n")
  })


}

#' Delete and initialize arrays associated with a REVEAL package
#'
#' @export
init_arrays = function(pkgEnv,
                       con,
                       arrays_to_init = NULL,
                       force = FALSE,
                       silent = FALSE,
                       allow_array_reinit = FALSE){
  db = con$db
  L = pkgEnv$meta$L

  all_arrays = get_entity_names(pkgEnv)
  if(!is.null(arrays_to_init)){
    arrays_to_init = intersect(all_arrays, arrays_to_init)
  }
  else{
    arrays_to_init = all_arrays
  }

  init_in_namespaces = sapply(arrays_to_init, function(x) find_namespace(pkgEnv = pkgEnv, con = con, entitynm = x))
  if(!all(init_in_namespaces %in% c("public", names(pkgEnv$meta$L$namespace)))){
    stop("Schema document contains arrays in namespaces not defined by schema document.  Check before continuing.")
  }

  if(!allow_array_reinit & !con$use_test_namespace){
      existing_array_fullnames = apply(X = iquery(con$db, "list(ns:all)", return=T), MARGIN = 1, FUN = function(x) paste0(x[["namespace"]],".",x[["name"]]))
      init_array_fullnames = sapply(arrays_to_init, function(x){full_arrayname(pkgEnv, x, con)})
      arrays_that_exist = intersect(init_array_fullnames, existing_array_fullnames)
      if(length(arrays_that_exist)>0 & !silent){
        message("Array reinitiatialization is disallowed.  Not initializing: ",paste(arrays_that_exist, collapse = ", "))
      }
      arrays_to_init = sapply(setdiff(init_array_fullnames,existing_array_fullnames), strip_namespace)
    }

  if (length(arrays_to_init) == 0) {cat("ERROR: Check array names\n"); return(FALSE)}

  if(!get_confirmation(paste0("CAUTION: The following arrays will be deleted and reinitialized\n",
                              paste(sapply(arrays_to_init, function(x){full_arrayname(pkgEnv, x, con)}), collapse = ", ")),force)){
    return(FALSE)
  }

  if (is_scidb_ee(con) & !is.null(PERMISSIONS_ARRAY(pkgEnv, con))) {
    if (identical(arrays_to_init, get_entity_names(pkgEnv))) {
      if (!force) {
        cat("You asked to initialize all arrays. Should I initialize the permissions array too?\n")
        resp_perm <- readline("(Y)es/(N)o: ")
      }
      else {
        resp_perm = "y"
      }
    }
    else { # do not reinitialize permissions array if only working on one or two arrays
      resp_perm = "n"
    }
  }
  else {
    # In CE mode, do not need a permissions array
    resp_perm = "n"
  }

  arrays = L$array

  # First clean up arrays
  for (name in arrays_to_init) {
    name = strip_namespace(name)
    arr = arrays[[name]]
    dims = arr$dims

    fullnm = full_arrayname(pkgEnv = pkgEnv, con = con, entitynm = name)
    if (!silent) message("Trying to remove array ", fullnm)
    tryCatch({iquery(db, paste("remove(", fullnm, ")"), force=TRUE)},
             error = function(e){if (!silent) cat("====Failed to remove array: ", fullnm, ",\n",sep = "")})
    info_flag = arr$infoArray
    if (!is.null(info_flag)) { if(info_flag){
      if (!silent) message("Trying to remove array ", fullnm, "_INFO")
      tryCatch({iquery(db, paste("remove(", fullnm, "_INFO)", sep = ""), force=TRUE)},
               error = function(e){if (!silent) cat("====Failed to remove", paste("remove array: ", fullnm, "_INFO\n", sep = ""))})
    }}
  }

  # Next create the arrays

  for (name in arrays_to_init) {
    name = strip_namespace(name)
    arr = arrays[[name]]
    dims = arr$dims
    if (class(dims) == "character") {dim_str = dims} else if (class(dims) == "list"){
      dim_str = yaml_to_dim_str(dims)
    } else {stop("Unexpected class for dims")}
    attr_str = yaml_to_attr_string(arr$attributes, arr$compression_on, arr$default_values)
    attr_str = paste("<", attr_str, ">")

    fullnm = full_arrayname(pkgEnv = pkgEnv, con = con, entitynm = name)
    tryCatch({
      query =       paste("create array", fullnm, attr_str, "[", dim_str, "]")
      if (!silent) message("running: ", query, "; and initializing empty version 1")
      iquery(db,
             query
      )
      iquery(db, paste0("store(", fullnm, ", ", fullnm, ")")) #163; storing empty state of array puts each array at version 1
    },
    error = function(e){cat("=== faced error in creating array:", fullnm, "\n")}
    )

    info_flag = arr$infoArray
    if (!is.null(info_flag)) { if(info_flag){
      #         if(arr$data_class == "data") {stop("array of class \"data\" cannot have INFO array")}
      tryCatch({
        # Info array
        if (is.null(arr$infoArray_max_keys)){
          key_str = "key_id"
        } else {
          key_str = paste("key_id=0:*,", arr$infoArray_max_keys, ",0", sep = "")
        }
        query = paste("create array ", fullnm, "_INFO <key: string, val: string> [",
                      dim_str, ", ", key_str, "]",
                      sep = "")
        if (!silent) message("running: ", query)
        iquery(db,
               query
        )
        iquery(db, paste0("store(", fullnm, "_INFO, ", fullnm, "_INFO)")) #163; storing empty state of array puts each array at version 1

      }, error = function(e){cat("=== faced error in creating array: ", fullnm, "_INFO\n", sep="")}
      )
    }}
    if ("default_value" %in% names(arr)) {
      default_value = arr[["default_value"]]
      tryCatch({
        query = paste0(
          "store(redimension(",default_value,", ",
          fullnm,
          "), ",
          fullnm,
          ")"
        )
        if (!silent) message("running: ", query)
        iquery(db,query)
      },
      error = function(e){cat("=== faced error initializing default value for array: ", fullnm, "\n", sep="")}
      )
    }
  }

  # Clean up any package cache
  if (!silent) message("Cleaning up any local cache values")
  pkgEnv$cache$lookup = list()
  cached_entities = get_entity_names(pkgEnv)[sapply(get_entity_names(pkgEnv), function(x){is_entity_cached(pkgEnv, x)})]
  for (entity in cached_entities) {
    pkgEnv$cache[[entity]] = NULL
  }

  if ( (tolower(resp_perm) == 'y' | tolower(resp_perm) == 'yes') & !is.na(resp_perm)) {
    cat("Proceeding with initialization of permissions array\n")
    init_permissions_array(pkgEnv = pkgEnv, con = con)
  } else{
    cat("Not initalizing permissions array\n")
  }
}

#' Initialize namespaces associated with a REVEAL package
#'
#' @export
init_namespaces = function(pkgEnv,
                           con,
                           namespaces_to_init = NULL,
                           force = FALSE,
                           silent = FALSE) {
  namespace = scidb::iquery(con$db, "list('namespaces')", T)
  namespace = setdiff(names(pkgEnv$meta$L$namespace), namespace$name)
  if(!is.null(namespaces_to_init)){
    namespace = intersect(namespace, namespaces_to_init)
  }

  if(length(namespace)==0) return(FALSE)
  if(!get_confirmation(paste0("The following namespaces will be initialized:\n ", paste(namespace, collapse = ", ")),force)){
    return(FALSE)
  }

  for(ns in namespace){
    tryCatch({
      query = paste0("create_namespace('", ns, "')")
      if (!silent) message("running: ", query, "")
      scidb::iquery(con$db, query)
    },
    error = function(e){cat("=== faced error in creating namespace:", ns, "\n")}
    )
  }
}

#' Initialize roles associated with a REVEAL package
#'
#' @export
init_roles = function(pkgEnv,
                      con,
                      roles_to_init = NULL,
                      force = FALSE,
                      silent = FALSE) {

  roles = scidb::iquery(con$db, "list('roles')", T)
  roles = setdiff(names(pkgEnv$meta$L$role), roles$name)
  if(!is.null(roles_to_init)){
    roles = intersect(roles, roles_to_init)
  }

  if(length(roles)==0) return(FALSE)
  if(!get_confirmation(paste0("The following roles will be initialized:\n ", paste(roles, collapse = ", ")),force)){
    return(FALSE)
  }

  for(role in roles){
    tryCatch({
      query = paste0("create_role('", role, "')")
      if (!silent) message("running: ", query, " and initializing permissions")
      scidb::iquery(con$db, query)
      ns_perms = pkgEnv$meta$L$role[[role]]$namespace_permissions
      for(ns in names(ns_perms)){
        scidb::iquery(con$db, paste0("set_role_permissions('", role, "', ", ns, ", '", ns_perms[[ns]], "')"))
      }
    },
    error = function(e){cat("=== faced error in creating role:", role, "\n")}
    )
  }
}

#' @export
check_roles = function(pkgEnv, con){
  roles_in_db = scidb::iquery(con$db, "list('roles')", T)$name
  roles_in_schema = names(pkgEnv$meta$L$role)
  missing_roles = setdiff(roles_in_schema, roles_in_db)
  correct=T
  if(length(missing_roles)>0){
    message("Roles not initialized: ", paste(missing_roles, collapse = ", "))
    correct=F
  } else {
    message("All roles initialized.")
  }
  for(role in intersect(roles_in_schema, roles_in_db)){
    perms_in_db = iquery(con$db, paste0("show_role_permissions('",role,"')"), T)
    perms_in_db = perms_in_db[perms_in_db$entity == "namespace",]
    perms_in_db = perms_in_db[perms_in_db$name %in% names(pkgEnv$meta$L$namespace),]
    perms_in_db$permissions_db = vapply(strsplit(perms_in_db$permissions, NULL), function(x) paste(sort(x), collapse = ''), '')
    perms_in_schema = revealcore:::show_role_permissions(pkgEnv, role)
    perms_in_schema$name = perms_in_schema$namespace
    perms_in_schema$permissions_schema = vapply(strsplit(perms_in_schema$permissions, NULL), function(x) paste(sort(x), collapse = ''), '')
    perms_compare = dplyr::full_join(perms_in_db[,c("name","permissions_db")],
                                      perms_in_schema[,c("name","permissions_schema")],
                                      by=c("name"))
    perms_wrong = perms_compare[is.na(perms_compare$permissions_db) |
                                is.na(perms_compare$permissions_schema) |
                                perms_compare$permissions_db != perms_compare$permissions_schema,]
    if(nrow(perms_wrong)!=0){
      message("Permissions mismatch for ", role, " role (namespace|db|schema): ",
              paste(sapply(1:nrow(perms_wrong),
                           function(x){paste(perms_wrong[x,],collapse="|")}),
                    collapse=", "))
      correct = F
    }
  }
  correct
}

#' @export
check_namespaces = function(pkgEnv, con){
  ns_in_db = scidb::iquery(con$db, "list('namespaces')", T)$name
  ns_in_schema = names(pkgEnv$meta$L$namespace)
  missing_ns = setdiff(ns_in_schema, ns_in_db)
  correct=T
  if(length(missing_ns)>0){
    message("Namespaces not initialized: ", paste(missing_ns, collapse = ", "))
    correct=F
  } else {
    message("All namespaces initialized.")
  }
  is_secured = sapply(ns_in_schema, function(x){is_namespace_secured(pkgEnv,x)})

  if(any(is_secured)){
    message("Ensure Scidb config has secure_scan configuration for pairs of namespaces and permissions arrays:\n\t",
            paste(sapply(names(which(is_secured)),
                         function(x){paste0(x, " <- ", PERMISSIONS_ARRAY(pkgEnv, con, x))}), collapse="\n\t"))
  }
  return(correct)
}

#' @export
check_arrays = function(pkgEnv, con){
  arrays_in_db = scidb::iquery(con$db, "list(ns:all)", T)
  arrays_in_db$full_arrayname = sapply(1:nrow(arrays_in_db),
                                       function(x){paste0(arrays_in_db[x,"namespace"],".",
                                                          arrays_in_db[x,"name"])})
  entities_in_schema = data.frame("name" = revealcore::get_entity_names(pkgEnv),
                                  "full_arrayname" = sapply(revealcore::get_entity_names(pkgEnv),
                                                            function(x){revealcore::full_arrayname(pkgEnv, x, con=con)}))
  missing_arrays = setdiff(entities_in_schema$full_arrayname,
                           arrays_in_db$full_arrayname)
  correct=T
  if(length(missing_arrays)>0){
    message("Arrays not initialized: ", paste(missing_arrays, collapse = ", "))
    correct=F
  } else {
    message("All arrays initialized.")
  }
  arrays_in_db = dplyr::inner_join(arrays_in_db, entities_in_schema, by=c("name","full_arrayname"))
  arrays_in_db$schema = gsub(arrays_in_db$schema, pattern = " ", replacement = "")
  arrays_in_db$schema_schema = sapply(arrays_in_db$name,
                                      function(x){gsub(revealcore:::get_entity_schema(pkgEnv, x, T),
                                                       pattern = " ",
                                                       replacement = "")})
  schema_wrong = arrays_in_db[arrays_in_db$schema_schema!=arrays_in_db$schema,]
  if(nrow(schema_wrong != 0)){
    correct=F
    message("Array schema mismatch for arrays (array|db|schema):\n\t",
            paste(sapply(1:nrow(schema_wrong),
                         function(x){paste(schema_wrong[x,c("full_arrayname","schema","schema_schema")],collapse="|")}),
                  collapse="\n\t"))
  }

  return(correct)
}

#' @export
check_db = function(pkgEnv, con){
  return(check_namespaces(pkgEnv, con) & check_roles(pkgEnv, con) & check_arrays(pkgEnv, con))
}
