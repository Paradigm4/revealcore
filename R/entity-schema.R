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
  "dataset_version" %in% get_idname(pkgEnv, entitynm)
}

#' @export
is_entity_cached = function(pkgEnv, entitynm) {
  val  = pkgEnv$meta$L$array[[entitynm]]$cached # read from SCHEMA file
  # if no value for cached, then entity is potentially not cached
  ifelse(is.null(val), FALSE, val)
}

#' @export
get_entity_data_class = function(pkgEnv, entitynm){
  pkgEnv$meta$L$array[[entitynm]]$data_class
}

#' @export
get_idname = function(pkgEnv, arrayname){
  local_arrnm = strip_namespace(arrayname)
  idname = pkgEnv$meta$L$array[[local_arrnm]]$dims
  if (class(idname) == "character") return(idname) else return(names(idname))
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
