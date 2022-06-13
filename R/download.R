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

#' @importFrom httr config progress http_error http_status write_disk GET
#' @importFrom urltools scheme
#' @importFrom aws.s3 get_bucketname get_objectkey save_object
NULL


#' Obtain file extention from path
#'
#' Returns anything after (and including) the first `.` found in basename
#' of path
#'
#' @param url Path to get extension for
#' @param last_dot_only if `TRUE` use last `.` to determine extension
#' @return File extension for given path
file_ext <- function(url, last_dot_only = FALSE) {
  if (!grepl("\\.", basename(url))) {
    ""
  } else if (identical(TRUE, last_dot_only)) {
    gsub("^(.*\\.)", ".", url)
  } else {
    gsub("^(.*/)?([^.]*)(.*)$", "\\3", url)
  }
}


#' Check whether or not `path` is absolute (i.e., starts with a `/`)
#'
#' @param path Path to check
#' @return Logical indicating absolute path
is_absolute <- function(path) {
  all(grepl("^/", path))
}


#' Return absolute path for a given path
#'
#' @param path Path to be made absolute
#' @return Normalize, absolute `path` relative to current working directory
absolute_path <- function(path) {
  if (!is_absolute(path)) {
    path <- file.path(getwd(), path)
  }
  return(normalizePath(gsub("//+", "/", path), mustWork = FALSE))
}


#' Download file using httr
#'
#' @param url Supported schemes are http and https
#' @param filename File name to save to (defaults to `tempfile()`)
#' @param ... Passed on to `httr::config()`
#' @param progress If set show download progress bar
#' @param ssl_no_verify Turn off SSL certificate and host verification (not recommended)
#' @return File name of saved download
#' @export
download_file <- function(url, filename = tempfile(fileext = file_ext(url)), ..., progress = TRUE, ssl_no_verify = FALSE) {
  stopifnot("single string expected for 'url'" = is.character(url) && 1 == length(url))
  stopifnot("unsupported URL scheme" = urltools::scheme(url) %in% c("http", "https"))

  opts <- NULL
  if (identical(TRUE, ssl_no_verify)) {
    opts <- c(httr::config(ssl_verifyhost = 0, ssl_verifypeer = 0), opts)
  }
  if (!identical(FALSE, progress)) {
    opts <- c(httr::progress(), opts)
  }
  filename <- absolute_path(filename)
  opts <- c(httr::write_disk(filename, overwrite = TRUE), opts)
  gc() # NOTE: unclear why this prevents "memory not mapped" error + segfault in curl call (R 4.x.x, CentOS 7)
  response <- httr::GET(url, opts)
  if (httr::http_error(response)) {
    unlink(filename)
    stop(httr::http_status(response)$message)
  }
  return(filename)
}


#' Download S3 object to file
#'
#' @param object Object name or s3 url
#' @param bucket Optional bucket (derived from object if missing)
#' @param opts Options passed on to `aws.s3::save_object()`
#' @param filename File name to save to (defaults to `tempfile()`)
#' @param ... Passed as options to `aws.s3::save_object()`
#' @param progress If set show download progress bar
#' @return File name of saved download
#' @export
download_s3 <- function(object, bucket, opts = NULL, filename = NULL, ..., progress = TRUE) {
  if (missing(bucket)) {
    bucket <- aws.s3::get_bucketname(object)
  }
  object <- aws.s3::get_objectkey(object)
  tmp <- if (is.character(filename)) {
    absolute_path(filename)
  } else {
    tempfile(fileext = file_ext(object))
  }
  tmp_preexists <- file.exists(tmp)
  opts <- c(opts, list(show_progress = !identical(FALSE, progress)), list(...))
  tryCatch(
    {
      gc() # NOTE: unclear why this prevents "memory not mapped" error + segfault in curl call (R 4.x.x, CentOS 7)
      if (is.null(opts)) {
        r <- aws.s3::save_object(bucket = bucket, object = object, file = tmp)
      } else {
        r <- do.call(aws.s3::save_object, c(list(
          bucket = bucket, object = object,
          file = tmp
        ), opts))
      }
    },
    silent = TRUE,
    error = function(...) {
      if (!tmp_preexists) {
        unlink(tmp)
      }
      stop(...)
    }
  )
  return(tmp)
}


#' Run function on file downloaded from S3
#'
#' Modified version of `aws.s3::s3read_using()`. The difference with the original
#' implementation is that `filename` is taken "verbatim" rather than made relative
#' to tempdir.
#'
#' @param FUN function accepting file path as first argument
#' @param object S3 object (i.e., object name or full s3://... url)
#' @param ... passed as extra arguments to `FUN`
#' @param bucket S3 bucket, uses bucket from `object` if missing
#' @param opts see `aws.s3::s3read_using()`
#' @param filename File name to save S3 object to (defaults to using `tempfile()`)
#' @param progress Toggle display of progress bar
#' @export
s3read_using <- function(FUN, object, ..., bucket, opts = NULL, filename = NULL, progress = TRUE) {
  if (missing(bucket)) {
    bucket <- aws.s3::get_bucketname(object)
  }
  object <- aws.s3::get_objectkey(object)
  tmp <- if (is.character(filename)) {
    filename
  } else {
    tempfile(fileext = file_ext(object))
  }
  if (!file.exists(tmp)) {
    on.exit(unlink(tmp))
  }
  download_s3(object = object, bucket = bucket, opts = opts, filename = tmp, progress = progress)
  return(FUN(tmp, ...))
}


#' Analogous to `s3read_using()` but switches between file path, http(s) and S3 automatically
#'
#'
#' @param FUN function accepting file path as first argument
#' @param uri Character string specifiying S3 URL, HTTP(S) URL or local file path
#' @param ... passed as extra arguments to `FUN`
#' @param filename File name to save object to (defaults to using `tempfile()`)
#' @param progress Toggle display of progress bar
#' @param ssl_no_verify Toggle SSL certificate verification (not recommended)
#' @export
uri_read_using <- function(FUN, uri, ..., filename = tempfile(fileext = file_ext(uri)),
                           progress = TRUE, ssl_no_verify = FALSE) {
  stopifnot("Single string expected for 'uri'" = is.character(uri) && 1 == length(uri))
  scheme <- urltools::scheme(uri)
  stopifnot("Unsupported URL scheme" = is.na(scheme) || scheme %in% c("http", "https", "s3"))
  filename <- absolute_path(filename)

  # Remove file if not preexisting
  if (!file.exists(filename)) {
    on.exit(unlink(filename))
  }

  # no scheme -- assume 'uri' is a local file
  if (is.na(scheme)) {
    stopifnot(file.exists(uri))
    FUN(uri, ...)
  }

  # S3 object -- use aws.s3
  else if ("s3" == scheme) {
    s3read_using(
      FUN = FUN, object = uri, filename = filename, progress = progress, ...
    )
  }

  # http/https -- use `download_file()`
  else {
    file <- download_file(uri, filename = filename, ssl_no_verify = ssl_no_verify, progress = progress)
    FUN(file, ...)
  }
}


#' Download file from uri
#'
#' Downloads file from S3 or http(s) to a file and returned the file name. If a file path is
#' specified, the path will be returned if the file exists.
#'
#' @param uri URI (i.e., file path or S3 or http(s) URL) specifying source location
#' @param filename File name (path) to store file at (defaults to `tempfile()`)
#' @param ... Forwarded to internal download functions
#' @param progress If set, show download progress while downloading
#' @return Character vector with file path where file was downloaded
#' @export
uri_download <- function(uri, filename = tempfile(fileext = file_ext(uri)), ..., progress = TRUE) {
  stopifnot("single string expected for 'uri'" = is.character(uri) && 1 == length(uri))
  scheme <- urltools::scheme(uri)
  stopifnot("unsupported URL scheme" = scheme %in% c("http", "https", "s3") || is.na(scheme))
  if (is.na(scheme)) {
    uri <- absolute_path(uri)
    stopifnot("File not found" = file.exists(uri))
    uri
  } else if ("s3" == scheme) {
    download_s3(uri, filename = filename, progress = progress, ...)
  } else {
    download_file(uri, filename = filename, progress = progress, ...)
  }
}


#' List "files" in file system or on S3
#'
#' Delegates to `base::list.files()` for file system paths and
#' mimics basic `base::list.files()` behavior for S3 paths.
#'
#' @param path File system path or S3 URL
#' @param pattern Filter pattern (regex)
#' @param full.names If set, return full path names rather than basenames
#' @param recursive List files recursively
#' @param ignore.case Ignore case for filter pattern
#' @param ... Forwarded to `base::list.files()` or `aws.s3::get_bucket_df()` for files and S3 respectively
#' @return Character vector of file names
#' @export
list_files <- function(path = ".", pattern = NULL,
                       full.names = FALSE, recursive = FALSE,
                       ignore.case = FALSE, ...) {
  stopifnot("Single string expected for 'path'" = is.character(path) && 1 == length(path))
  scheme <- urltools::scheme(path)
  stopifnot("Unsupported scheme" = is.na(scheme) || scheme %in% "s3")
  ret <- if (is.na(scheme)) {
    ## Path is a file path -- delegate to base::list.files()
    gsub("//*", "/", list.files(
      path = path, pattern = pattern, full.names = full.names,
      recursive = recursive, ignore.case = ignore.case,
      ...
    ))
  } else {
    ## Path is a S3 path -- obtain keys from aws.s3::get_bucket_df() and filter as appropriate
    prefix <- urltools::path(path)
    if (is.na(prefix)) {
      prefix <- ""
    } else {
      prefix <- paste0(gsub("//*$", "", prefix), "/")
    }
    keys <- aws.s3::get_bucket_df(path, prefix = prefix, ...)$Key
    if (!identical(TRUE, recursive)) {
      no_recurse_filter <- glue::glue("^{rex::escape(prefix)}([^/]+)$")
      keys <- grep(no_recurse_filter, keys, value = TRUE)
    }
    if (!is.null(pattern)) {
      keys <- grep(pattern, keys, value = TRUE, ignore.case = ignore.case)
    }
    if (!identical(TRUE, full.names)) {
      keys <- basename(keys)
    } else {
      keys <- glue::glue("{scheme}://{urltools::domain(path)}/{keys}")
    }
    keys
  }
  return(ret)
}
