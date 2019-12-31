# Evaluation
Evaluating the efficiency and flexibility of SciFlow as opposed to carrying out similar tasks;

1) **Without Channel Cordination - Go Control Thread**
   
   Navigate to the NoGo/workflow/ folder and run the following commannd on the terminal.
   ```console
   time ./runWithoutGo.sh
   ```
3) **Without Implicit Parallelism**
   
   Navigate to the NoParslGo/ folder and run the following commannd on the terminal.
   ```console
   go run test.go 
   ```
2) **Without both Implicit Parallelism and Channel Cordination - Parsl and Go Control Thread**
   
   Navigate to the NoParslGo/workflow/ folder and run the following commannd on the terminal. 
   ```console   
   time ./run.sh
   ```


