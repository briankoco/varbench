c_args <- commandArgs(trailingOnly = TRUE)
if (length(c_args) < 3){
  print("Usage: compute_cv.R <rank> <infile> <outfile>")
  stop()
}

rank_max <- strtoi(c_args[1])
infile <- c_args[2] 
outfile <- c_args[3]

iterations_max <- 99
guess <- 30000000

library(readr)
syscall_data <- read_csv(infile, guess_max = guess)
syscall_data <- syscall_data[, c("rank","iteration","program_id","nsecs")]
prog_names <- levels(factor(unlist(syscall_data[,c("program_id")])))

rank <- vector()
iteration <- vector()
program_id <- vector()
nsecs <- vector()

for (iter in 0 : iterations_max){
  sub1 <- syscall_data[which(syscall_data$iteration == iter),]
  for (name in prog_names){
   sub2 <- sub1[which(sub1$program_id == name),]
   for (r in 0 : rank_max){
      iteration <- c(iteration, iter)
      program_id <- c(program_id, name)
      rank <- c(rank, r)
      
      times <- sub2[which(sub2$rank == r),]$nsecs
      
      nsecs <- c(nsecs, sum(times))
      
    }
  }
}

exec_time.data <- data.frame(iteration, program_id, rank, nsecs, stringsAsFactors = FALSE)

program_id2 <- vector()
coeff_var <- vector()

for (name in prog_names){
  program_id2 <- c(program_id2, name)
  times <- exec_time.data[which(exec_time.data$program_id == name),]$nsecs
  coeff_var <- c(coeff_var, sd(times) / mean(times))
}

cv.data <- data.frame(program_id2, coeff_var, stringsAsFactors = FALSE)
#Write Data Out
write.csv(cv.data, outfile, row.names = FALSE, quote = FALSE)
