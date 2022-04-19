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

#' @importFrom utils packageName
NULL

#' Get path to P4 logo image (transparent png, for use with light background)
p4logo <- function() {
  system.file("extdata", "p4-logo.png", package = packageName())
}
