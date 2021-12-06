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

#' @export
init_permissions_array = function(pkgEnv, con) {
  db = con$db

  permissions_arr = PERMISSIONS_ARRAY(pkgEnv, con)
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
  tryCatch({
    iquery(db, paste0("create array ", permissions_arr, " <access:bool> [user_id=0:*:0:1; dataset_id=0:*:0:256]"))
    iquery(db, paste0("store(", permissions_arr, ", ", permissions_arr, ")"))
  },
  error = function(e) {
    cat("====Failed to create permissions array\n")
  })


}

#' Initialize arrays associated with a REVEAL package
#'
#' @export
init_arrays = function(pkgEnv, con,
                       arrays_to_init = NULL,
                       pattern_array_names = NULL,
                       allow_array_reinit = FALSE,
                       force = FALSE,
                       silent = FALSE){
  db = con$db
  L = pkgEnv$meta$L

  if ((is.null(arrays_to_init) & is.null(pattern_array_names)) |
      (!is.null(arrays_to_init) & !is.null(pattern_array_names))) {
    stop("Only one of the two parameters can have a non-null value:
         `arrays_to_init` or `pattern_array_names`.
         Both cannot be null.")
  }
  if (!is.null(arrays_to_init)) {
    arrays_to_init = arrays_to_init[arrays_to_init %in% names(L$array)]
  }
  else if (!is.null(pattern_array_names)) {
    stopifnot(length(pattern_array_names) == 1)
    arrays_to_init = names(L$array)[grep(pattern = pattern_array_names,
                                         x = names(L$array))]
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

  cat("CAUTION: The following arrays will be deleted and reinitialized\n",
      paste(sapply(arrays_to_init, function(x){full_arrayname(pkgEnv, x, con)}), collapse = ", "), "\n Proceed?")
  if (!force) {
    response <- readline("(Y)es/(N)o: ")
  }
  else {
    response = 'yes'
  }
  if ( (tolower(response) == 'y' | tolower(response) == 'yes') & !is.na(response)) {
    cat("Proceeding with initialization of DB\n")
  }
  else{
    cat("Canceled initialization of DB\n")
    return(FALSE)
  }

  if (is_scidb_ee(con)) {
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
    attr_str = yaml_to_attr_string(arr$attributes, arr$compression_on)
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
    default_value = arr$default_value
    if (!is.null(default_value)){
      stop("TODO: Initialize default value")
    }
  }

  # Clean up any package cache
  if (!silent) message("Cleaning up any local cache values")
  .ghEnv$cache$lookup = list()
  cached_entities = get_entity_names(pkgEnv)[sapply(get_entity_names(pkgEnv), function(x){is_entity_cached(pkgEnv, x)})]
  for (entity in cached_entities) {
    .ghEnv$cache[[entity]] = NULL
  }

  if ( (tolower(resp_perm) == 'y' | tolower(resp_perm) == 'yes') & !is.na(resp_perm)) {
    cat("Proceeding with initialization of permissions array\n")
    init_permissions_array(con = con)
  } else{
    cat("Not initalizing permissions array\n")
  }
}

#' Initialize namespaces associated with a REVEAL package
#'
#' @export
init_namespaces = function(pkgEnv, con, namespaces_to_init = NULL) {
  namespace = scidb::iquery(con$db, "list('namespaces')", T)
  namespace = setdiff(names(schema$namespace), namespace$name)
  if(!is.null(namespaces_to_init)){
    namespace = intersect(namespace, namespaces_to_init)
  }

  message(paste0("Creating namespaces: ", paste(namespace, collapse = ", ")))

  for(ns in namespace){
    scidb::iquery(paste0("create_namespace('", ns, "')"))
  }
}

#' Initialize roles associated with a REVEAL package
#'
#' @export
init_roles = function(pkgEnv, con, roles_to_init = NULL) {

  roles = scidb::iquery(con$db, "list('roles')", T)
  roles = setdiff(names(pkgEnv$meta$L$role), roles$name)
  if(!is.null(roles_to_init)){
    roles = intersect(roles, roles_to_init)
  }

  message(paste0("Creating and assigning permissions for roles: ", paste(roles, collapse = ", ")))

  for(role in roles){
    scidb::iquery(paste0("create_role('", role, "')"))
    ns_perms = pkgEnv$meta$L$role[[role]]$namespace_permissions
    for(ns in names(ns_perms)){
      scidb::iquery(paste0("set_role_permissions('", role, "', '", ns, "', '", ns_perms[[ns]], "')"))
    }
  }
}
