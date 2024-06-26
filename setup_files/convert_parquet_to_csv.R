#!/usr/bin/Rscript --vanilla

required_packages = c(
  "arrow",
  "tidyverse",
  "glue"
)

installed_packages = rownames(installed.packages())
packages_to_install = required_packages[!(required_packages %in% installed_packages)]

if (length(packages_to_install) > 0) {
  install.packages(
    packages_to_install,
    dependencies = TRUE,
    repos = "https://cloud.r-project.org",
  )
}

suppressPackageStartupMessages({
  library(arrow)
  library(tidyverse)
  library(glue)
})

command_args = commandArgs(trailingOnly = TRUE)
parquet_filename = command_args[1]
csv_filename = str_replace(parquet_filename, ".parquet$", ".csv")

rows = read_parquet(parquet_filename)
print(glue("Read {nrow(rows)} rows from {parquet_filename}"))

if ("__index_level_0__" %in% names(rows)) {
  rows = select(rows, -"__index_level_0__")
}

write_csv(rows, csv_filename, na = "")
print(glue("Wrote {nrow(rows)} rows to {csv_filename}"))
