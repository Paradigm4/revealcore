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
# Helper functions for dataframe / text manipulation
############################################################

#' @export
strip_namespace = function(arrayname) sub("^.*[.]", "", arrayname)

#' @export
get_namespace = function(arrayname) sub("[.].*$", "", arrayname)

#' @export
is_scidb_ee = function(con) !con$scidb_ce

#' @export
is_scidb_ce = function(con) con$scidb_ce

get_confirmation = function(message, force=FALSE){
  cat(paste0(message,"\n"))
  if (!force) {
    response <- readline(" Proceed? (Y)es/(N)o: ")
  }
  else {
    response = 'yes'
  }
  if ( (tolower(response) == 'y' | tolower(response) == 'yes') & !is.na(response)) {
    cat("Proceeding\n")
    return(TRUE)
  }
  else{
    cat("Canceled\n")
    return(FALSE)
  }
}

#' Pretty print a large vector of strings, integers etc.
#'
#' @param vec vector that is to be pretty printed
#' @param prettify_after prettify output if length of vector is longer than this limit
#' @export
pretty_print = function(vec, prettify_after = 7) {
  prettify_after = ifelse(prettify_after >= 7, prettify_after, 7) # force parameter to have a minimum value of 7
  ifelse(length(vec) <= prettify_after,
         paste(vec, collapse = ", "),
         paste(pretty_print(head(vec, ceiling((prettify_after-3)/2))),
               "...(Total: ", length(vec), ")... ",
               pretty_print(tail(vec, ceiling((prettify_after-3)/2))),
               sep = ""))}

#' Drop columns that have NA in all rows
#'
#' @examples {
#' drop_na_columns(data.frame(a = 1:3, b = NA, c = c('x', 'y', 'z')))
#' }
#' @export
drop_na_columns = function(df){
  if (nrow(df) > 0) {
    if( "data.table" %in% class(df) |
        (nrow(df) == 1 & ncol(df) == 1)) {
      # Use a different method to remove NA columns if a data.table
      # http://stackoverflow.com/questions/2643939/remove-columns-from-dataframe-where-all-values-are-na
      base::Filter(function(x)
        !all(is.na(x)),
        df)

    } else {
      df[,colSums(is.na(df))<nrow(df)]
    }
  } else {
    df
  }
}

#' helper function to do the equivalent of match between two data frames
#'
#' @param df1 the values to be matched.
#' @param df2 the values to be matched against
#'
#' @return matched indices
#'
#' @export
match_df = function(df1, df2){
  df1$row_index_df1 = 1:nrow(df1)
  df2$row_index_df2 = 1:nrow(df2)
  df_merge = merge(df1, df2, all.x=T)
  df_merge[order(df_merge$row_index_df1),]$row_index_df2
}

#' helper function to report matches between vectors
#'
#' @param source source vector for finding matches from
#' @param target target vector in which to find matches
#' @param match_fcn a function that does matching between source and target.  Default base::match. Use revealcore::match_df for data frames.
#'
#' @return
#' list(match_res, source_matched_idx, source_unmatched_idx, target_matched_idx)
#' @export
find_matches_and_return_indices = function(source, target, match_fcn = base::match){
  match_res = match_fcn(source, target)
  match_idx = which(!is.na(match_res))
  non_match_idx = which(is.na(match_res))

  list(match_res = match_res,
       source_matched_idx = match_idx,
       source_unmatched_idx = non_match_idx,
       target_matched_idx = match_res[match_idx])
}

#' helper function to merge multiple matches between vectors from \link{find_matches_and_return_indices}
#'
#' @param m1 primary result from \link{find_matches_and_return_indices}
#' @param m2 secondary match result, used for instances where m1 is unmatched
#'
#' @return
#' list(match_res, source_matched_idx, source_unmatched_idx, target_matched_idx)
#' @export
merge_find_matches_and_return_indices = function(m1, m2){
  m3 = list()
  m3$match_res = m1$match_res
  m3$match_res[is.na(m3$match_res)] = m2$match_res[is.na(m3$match_res)]
  m3$source_matched_idx = which(!is.na(m3$match_res))
  m3$source_unmatched_idx = which(is.na(m3$match_res))
  m3$target_matched_idx = m3$match_res[m3$source_matched_idx]
  return(m3)
}

#' @export
data_type_mapping_r_to_scidb = setNames(
  c('int64',   'string',    'numeric', 'logical'),
  c('integer', 'character', 'float',   'bool')
)

#' @export
data_type_mapping_scidb_to_r = setNames(
  c(names(data_type_mapping_r_to_scidb),        'integer'),
  c(as.character(data_type_mapping_r_to_scidb), 'int32')
)
