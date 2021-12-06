.onAttach = function(libname, pkgname)
{
  packageStartupMessage("Paradigm4 utilities" ,
                        domain = NULL, appendLF = TRUE)
  options("scidb.aio"=TRUE)
}

# A global environment used to store the metadata information, and cache state by some functions
#' @export
.rcEnv = new.env(parent = emptyenv())

.rcEnv$meta$L = yaml.load_file(system.file("extdata", "SCHEMA.yaml", package="revealcore"))

.rcEnv$cache$lookup = list()
.rcEnv$cache[["GENE_SYMBOL"]] = NULL
