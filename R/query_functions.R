#' Get entity, optionally filtered by a dataframe of id(s)
#'
#' @param entitynm Entity name
#' @param full_arrayname Full name of an array, or an AFL expression to filter on (e.g., something wrapped in secure_scan)
#' @param id data.frame or arrayOp containing ID values to filter by
#' @param return_aop if FALSE (default), return a dataframe.  if TRUE, return an arrayop expression.
#' @param update_cache if entitynm is a cached entity (see \link{is_entity_cached}), force update the cache.  Applies only if `return_aop` is FALSE and `id` is NULL.  The cache is updated regardless if the previous condition holds and the cache for the entity is empty.
#'
#' @return arrayop query on entity or the corresponding dataframe
#'
#' @export
get_entity <- function(pkgEnv,
                       con,
                       entitynm,
                       id = NULL,
                       return_aop = FALSE,
                       update_cache=F) {
  stopifnot(is.data.frame(id) | is.null(id))
  stopifnot(is_entity(pkgEnv, entitynm))
  if(is_entity_cached(pkgEnv = pkgEnv, entitynm = entitynm) & !return_aop){
    cached_arr = get_entity_from_cache(pkgEnv, entitynm, id)
    if(!is.null(cached_arr)){
      return(cached_arr)
    }
  }
  full_arrayname = scan_entity(pkgEnv=pkgEnv, entitynm = entitynm, con=con)
  ret <- con$aop_connection$afl_expr(full_arrayname)
  if(!is.null(id)){
    ret <- ret$semi_join(id)
  }
  if(!return_aop){
    ret <- ret$to_df_all()
    if(is.null(id) & is_entity_cached(pkgEnv, entitynm) & (update_cache | is.null(pkgEnv$cache[[entitynm]]))){
      pkgEnv$cache[[entitynm]] = ret
    }
  }
  return(ret)
}

get_entity_from_cache <- function(pkgEnv,
                                  entitynm,
                                  id = NULL){
  arr = pkgEnv$cache[[entitynm]]
  if(!is.null(arr) & !is.null(id)){
    return(dplyr::left_join(id, arr, by = colnames(id)))
  }
  return(arr)
}
