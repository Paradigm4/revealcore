#
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
#

############################################################
# Helper functions for using YAML schema object

yaml_to_dim_str = function(dims, for_auto_chunking=FALSE){
  if (!for_auto_chunking) {
    paste(
      names(dims), "=",
      sapply(dims, function(x) {paste(x$start, ":",
                                      ifelse(x$end == Inf, "*", x$end), ",", x$chunk_interval, ",",
                                      x$overlap, sep = "")}),
      sep = "", collapse = ", ")
  } else {
    paste0(names(dims), collapse = ",")
  }
}

yaml_to_attr_string = function(attributes, compression_on = FALSE){
  if (!compression_on) {
    paste(names(attributes), ":", attributes, collapse=" , ")
  } else {
    paste(names(attributes), ":", attributes, "COMPRESSION 'zlib'", collapse=" , ")
  }
}

#' Get entity id
#'
#' Get entity id from entity name
#'
#' @export
get_entity_id = function(pkgEnv, entitynm){
  entity_names = get_entity_names(pkgEnv)
  if (!all(entitynm %in% entity_names)) {
    stop("The following are not valid entities: ",
         pretty_print(entitynm[!(entitynm %in% entity_names)]))
  }
  if (length(entitynm) >= 1) {
    sapply(pkgEnv$meta$L$array[entitynm], function(elem) elem$entity_id)
  } else {
    stop("Expect entity to be vector or length 1 or more")
  }
}

#' Get entity name
#'
#' Get entity name from entity id
#'
#' @export
get_entity_from_entity_id = function(pkgEnv, entity_id) {
  entity_names = get_entity_names(pkgEnv)
  entity_id_lookup = get_entity_id(pkgEnv, entitynm = entity_names)
  m1 = find_matches_and_return_indices(
    source = entity_id,
    target = entity_id_lookup
  )
  if (length(m1$source_unmatched_idx) > 0) {
    stop("No entity for entity id: ",
         pretty_print(entity_id[m1$source_unmatched_idx]))
  }
  entities = names(entity_id_lookup[m1$target_matched_idx])
  stopifnot(all(entities %in% entity_names))
  entities
}

#' Get a list of all entity names
#'
#' @export
get_entity_names = function(pkgEnv, data_class = NULL){
  entities = names(pkgEnv$meta$L$array)
  if (!is.null(data_class)) {
    matches = sapply(entities, function(entity) {
      ifelse(get_entity_data_class(pkgEnv, entity) == data_class, TRUE, FALSE)
    })
    entities = entities[matches]
  }
  entities
}

#' @export
is_entity_secured = function(pkgEnv, entitynm, con=NULL){
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  entitynm = strip_namespace(entitynm) # extra QC
  nmsp = find_namespace(pkgEnv, entitynm, con)
  if (is.null(nmsp)) stop("unexpected namespace output")
  if(nmsp=="public"){
    return(FALSE)
  }
  return(pkgEnv$meta$L$namespace[[nmsp]]$is_secured)
}

#' @export
is_entity_versioned = function(pkgEnv, entitynm){
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  "dataset_version" %in% get_idname(pkgEnv, entitynm)
}

#' @export
is_entity_cached = function(pkgEnv, entitynm) {
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  val  = pkgEnv$meta$L$array[[entitynm]]$cached
  ifelse(is.null(val), FALSE, val)
}

#' @export
get_entity_data_class = function(pkgEnv, entitynm){
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  pkgEnv$meta$L$array[[entitynm]]$data_class
}

#' @export
get_entity_class = function(pkgEnv, entitynm) {
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  pkgEnv$meta$L$array[[entitynm]]$data_class
}

#' @export
get_idname = function(pkgEnv, entitynm){
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  idname = pkgEnv$meta$L$array[[entitynm]]$dims
  if (class(idname) == "character") return(idname) else return(names(idname))
}

#' @export
get_int64fields = function(pkgEnv, entitynm){
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  attr_types = unlist(pkgEnv$meta$L$array[[entitynm]]$attributes)
  int64_fields = names(attr_types[which(!(attr_types %in%
                                            c('string', 'datetime', 'int32', 'double', 'bool')))])
  stopifnot(all(unique(attr_types[int64_fields]) %in% c("int64", "numeric")))
  int64_fields
}

#' @export
get_search_by_entity = function(pkgEnv, entitynm) {
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  pkgEnv$meta$L$array[[entitynm]]$search_by_entity
}

#' @export
get_delete_by_entity = function(pkgEnv, entitynm) {
  stopifnot(entitynm %in% get_entity_names(pkgEnv))
  pkgEnv$meta$L$array[[entitynm]]$delete_by_entity
}

#' full name of array with namespace
#'
#' @inheritParams find_namespace
#'
#' @return the full name of the entity including namespace, e.g. 'gh_public.DATASET'
#'
#' @export
full_arrayname = function(pkgEnv, entitynm, con = NULL) {
  paste0(find_namespace(pkgEnv, entitynm, con), ".", entitynm)
}

#' full name of array with namespace, wrapped in secure_scan if the current user needs secure_scan to read the array
#'
#' @inheritParams find_namespace
#'
#' @return the full name of the entity including namespace and potentially wrapped in secure_scan, e.g. `gh_secure.DATASET` or `secure_scan(gh_secure.DATASET, strict:false)`
#'
#' @export
scan_entity = function(pkgEnv, entitynm, con){
  namespace = find_namespace(pkgEnv, entitynm, con)
  permissions = show_user_namespace_permissions(pkgEnv = pkgEnv,
                                                con = con,
                                                namespace = namespace)
  array = paste0(namespace, ".", entitynm)
  if(permissions$l & !permissions$r & pkgEnv$meta$L$namespace[[namespace]]$is_secured){
    if(con$aop_connection$scidb_version()$major>=21){
      array = paste0("secure_scan(",array,", strict:false)")
    } else {
      array = paste0("secure_scan(",array,")")
    }
  }
  array
}

#' return the namespace of an entity
#'
#' @param pkgEnv the package environment
#' @param entitynm an entity name, e.g. 'DATASET'
#' @param con if NULL, return the name according to the package schema document. If provided, return the name used by the connection
#'
#' @return the namespace of the entity, e.g. 'gh_public'
#'
#' @export
find_namespace = function(pkgEnv, entitynm, con = NULL) {
  ifelse(is.null(con),
         pkgEnv$meta$L$array[[entitynm]]$namespace,
         ifelse(con$scidb_ce,
                "public",
                ifelse(con$use_test_namespace,
                       pkgEnv$meta$L$package$test_namespace,
                       pkgEnv$meta$L$array[[entitynm]]$namespace)
                )
         )
}

#' return the full name of the permissions array for a given schema
#'
#' @inheritParams find_namespace
#'
#' @export
PERMISSIONS_ARRAY = function(pkgEnv, con=NULL) {
  arr = pkgEnv$meta$L$package$secure_dimension
  if(is.null(con)){
    nmspace = pkgEnv$meta$L$package$permissions_namespace
  }
  else if(is_scidb_ce(con)){nmspace = "public"}
  else if(con$use_test_namespace){nmspace = pkgEnv$meta$L$package$test_namespace}
  else{nmspace = pkgEnv$meta$L$package$permissions_namespace}

  if(!is.null(arr) & !is.null(nmspace)){
    return(paste0(nmspace, ".", arr))
  }
  return(NULL)
}

find_max_base_id_for_array = function(con, array_name, base_idname) {
  curr_max = scidb::iquery(con$db, glue::glue("aggregate(apply({array_name}, idx, {base_idname}), max(idx))"), return = T)$idx_max
  if (is.na(curr_max)) curr_max = -1
  curr_max
}
