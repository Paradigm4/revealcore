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
