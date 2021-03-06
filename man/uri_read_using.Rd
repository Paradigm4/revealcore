% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/download.R
\name{uri_read_using}
\alias{uri_read_using}
\title{Analogous to \code{s3read_using()} but switches between file path, http(s) and S3 automatically}
\usage{
uri_read_using(
  FUN,
  uri,
  ...,
  filename = tempfile(fileext = file_ext(uri)),
  progress = TRUE,
  ssl_no_verify = FALSE
)
}
\arguments{
\item{FUN}{function accepting file path as first argument}

\item{uri}{Character string specifiying S3 URL, HTTP(S) URL or local file path}

\item{...}{passed as extra arguments to \code{FUN}}

\item{filename}{File name to save object to (defaults to using \code{tempfile()})}

\item{progress}{Toggle display of progress bar}

\item{ssl_no_verify}{Toggle SSL certificate verification (not recommended)}
}
\description{
Analogous to \code{s3read_using()} but switches between file path, http(s) and S3 automatically
}
