#!/usr/bin/env python

"""
	Script to add syscall benchmarks to the existing libsyzcorpus
"""

header ="""
#include \"libsyzcorpus.h\"

"""

#Do this later
def usage():
	return

def parse_file(program_src):
	with open(program_src, "r") as f:
		lines = f.readlines()
		s=[]
		s.append(header)
		prev_line=""
		for line_number, line in enumerate(lines):
			if "memset(r, -1, sizeof(r));" in line:
				line = ""#delete this line
			if "long r[" in line:
				r_idx = int(line.split()[1].strip('r[').strip('];\n'))
				line = ""#delete the line
				line+="  " + "*num_calls = " + str(r_idx) + ";\n";
				line ="  " + "struct timespec start, stop;\n" + line	
			if 	("int _" in line) and ("(void) {" in line) and ("#include <string.h>" in prev_line): 
				line = line.replace("void", "vb_syscall_info_t * scall_info, int * num_calls")
			#Replace every instance of 'r[i]' with 'scall_info[i].ret_val'
			if "r[" in line:
				line = line.replace("r[", "scall_info[")
				line = line.replace("]", "].ret_val")
			if "= syscall(" in line:
				r_idx = int(line.split()[0].strip('scall_info[').strip('].ret_val'))
				syscall_num = line.split()[2].strip("syscall(").strip(",").strip(';)')
				line ="    "+"clock_gettime(CLOCK_MONOTONIC, &start);\n"+ line
				line+="    "+"clock_gettime(CLOCK_MONOTONIC, &stop);\n"
				#line+="    "+"scall_info["+str(r_idx)+"].nsecs = TO_NSECS(stop.tv_sec,stop.tv_nsec) - TO_NSECS(start.tv_sec,start.tv_nsec);\n"
				line+="    "+"scall_info["+str(r_idx)+"].time_in = TO_NSECS(start.tv_sec,start.tv_nsec);\n"
				line+="    "+"scall_info["+str(r_idx)+"].time_out = TO_NSECS(stop.tv_sec,stop.tv_nsec);\n"
				line+="    "+"scall_info["+str(r_idx)+"].syscall_number = " + syscall_num + ";\n"
			s.append(line)
			prev_line = line
	
	return ''.join(s) + '\n' 

if __name__ == "__main__":
	s= parse_file("../src/kernels/corpuses/sample-corpus/libsyzcorpus.c");
	print s
