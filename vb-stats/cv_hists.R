library(readr)
c_args <- commandArgs(trailingOnly = TRUE)
if (length(c_args) == 0){
  print("Usage: cv_hists.R <dir>")
  stop()
}

path <- paste(getwd(), c_args[1], sep="/")
files <- list.files(path = path, pattern = "*.csv", full.names = FALSE)

f <- paste(path, files[1], sep = "/")
temp <- read_csv(f, col_types = cols(
  program_id2 = col_character(),
  coeff_var = col_double()
))

program_names <- temp$program_id2

for(file in files){
  full_name <- paste(path, file, sep = "/")
  f_other <- read_csv(full_name, col_types = cols(
    program_id2 = col_character(),
    coeff_var = col_double()
  ))
  other_names <- f_other$program_id2
  program_names <- intersect(program_names, other_names)
}

print(paste("Subset of", length(program_names), "programs", sep = " "))

#We have subset now we can make some plots
for(file in files){
  data <- read_csv(paste(path, file, sep = "/"),col_types = cols(
    program_id2 = col_character(),
    coeff_var = col_double()
  ))
  data <- data[match(program_names, data$program_id2),]$coeff_var
  hist(data, probability = TRUE, main = file)
}


