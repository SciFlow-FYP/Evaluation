package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"io/ioutil"
	"log"
	"time"
	"path/filepath"
	"encoding/csv"
	"io"
)

func pythonCall(progName string, inChannel chan <- string, workflowNumber string) {
	cmd := exec.Command("python3", progName, workflowNumber)
	out, err := cmd.CombinedOutput()
	log.Println(cmd.Run())

	if err != nil {
		fmt.Println(err)
		// Exit with status 3.
    os.Exit(3)
	}
	fmt.Println(string(out))
	//check if msg is legit
	msg := string(out)[:len(out)-1]
	//msg := ("Module Completed: " + progName)
	inChannel <- msg
}


func integratePythonCall(progName string, inChannel1 chan <- string, inChannel2 chan <- string, workflowNumber string) {
	cmd := exec.Command("python3", progName, workflowNumber)
	out, err := cmd.CombinedOutput()
	log.Println(cmd.Run())

	if err != nil {
		fmt.Println(err)
		// Exit with status 3.
    os.Exit(3)
	}
	fmt.Println(string(out))
	//check if msg is legit
	msg := string(out)[:len(out)-1]
	//msg := ("Module Completed: " + progName)
	inChannel1 <- msg
	inChannel2 <- msg
}


func simplePythonCall1(progName string){
	cmd := exec.Command("python3", progName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os. Stderr
	log.Println(cmd.Run())
}

//=========================functions for rf==========================

type Accuracy_class_rf struct {
    Estimators int64 `json:"Estimators"`
    Depth int64 `json:"Depth"`
    Split int64 `json:"Split"`
    MaxFeatures int64 `json:"MaxFeatures"`
    Accuracy float64 `json:"Accuracy"`
}

func FindMaxAccuracy_rf(Accuracy_set []Accuracy_class_rf) (max Accuracy_class_rf) {

	max = Accuracy_set[0]
	for _, accuracy_obj := range Accuracy_set {
		if accuracy_obj.Accuracy > max.Accuracy {
			max = accuracy_obj
		}
	}
	return max
}

func Display_rf(accuracy_obj Accuracy_class_rf){
	fmt.Println("Estimators: ", accuracy_obj.Estimators)
	fmt.Println("Depth: ", accuracy_obj.Depth)
	fmt.Println("Split: ", accuracy_obj.Split)
	fmt.Println("MaxFeatures: ", accuracy_obj.MaxFeatures)
	fmt.Println("Accuracy: ", accuracy_obj.Accuracy)
}

func accuracySelection_rf (inChannel chan <- string, workflowNumber int) {
	fmt.Println("Accuracy selection for RF started")
	var files []string

	cmd := exec.Command("python", "-c", "from workflow import userScript; print userScript.outputLocation" + strconv.Itoa(workflowNumber))
	out, err := cmd.CombinedOutput()

	if err != nil {
		fmt.Println(err)
		// Exit with status 3.
		os.Exit(3)
	} else if out == nil{
		os.Exit(3)
	}
	root := string(out)[:len(out)-1]  + "rf/"
	accuracyJsonFile := string(out)[:len(out)-1] + "rf.json"


	/*
	cmd := exec.Command("python", "-c", "from workflow import userScript; print userScript.outputLocation3")
	out, err0 := cmd.CombinedOutput()
	if err0 != nil {
		fmt.Println(err0)
		// Exit with status 3.
    		os.Exit(3)
	}

	path := string(out)
    	subFolder := fmt.Sprintf("%s%s", path, "kmeans/")
	*/
	//root := "/home/mpiuser/Documents/FYP/gdelt/rf/"
	//root := "/home/amanda/FYP/gdelt/rf/"
  err1 := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
  files = append(files, path)
  return nil})

	fmt.Println(files)
  if err1 != nil {
  	panic(err1)
  }

	var Accuracy_set []Accuracy_class_rf

  for _, file := range files {
  	//if directory ignore
		fi, err2 := os.Stat(file)
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		var mode = fi.Mode();
		if mode.IsDir() == true {
			continue
		}


		csvFile, _ := os.Open(file)

    reader := csv.NewReader(bufio.NewReader(csvFile))

		for {
			line, error := reader.Read()
			if error == io.EOF {
			    break
			} else if error != nil {
			    log.Fatal(error)
			}
			var estimators int64
			var depth int64
			var split int64
			var maxfeatures int64
			var accuracy float64
			estimators, _ = strconv.ParseInt(line[0],10,0)
			depth, _ = strconv.ParseInt(line[1],10,0)
			split, _ = strconv.ParseInt(line[2],10,0)
			maxfeatures, _ = strconv.ParseInt(line[3],10,0)
			accuracy, _ = strconv.ParseFloat(line[4],64)
			//fmt.Println(reflect.TypeOf(newc))
			Accuracy_set = append(Accuracy_set, Accuracy_class_rf{
			    Estimators: estimators,
			    Depth: depth,
			    Split: split,
			    MaxFeatures: maxfeatures,
			    Accuracy: accuracy,
			})
		}

		//Accuracy_set = removeIt(Accuracy_class{"No_of_clusters", "Accuracy"}, Accuracy_set)
	}

	var max = FindMaxAccuracy_rf(Accuracy_set)
	writeAccuracyFile_rf(max, accuracyJsonFile)
	Display_rf(max)
	//fmt.Println(display)

	msg:= "Best accuracy selection for rf done"
	inChannel <- msg

}

func writeAccuracyFile_rf(accuracy_obj Accuracy_class_rf, accuracyJsonFile string) {

    accuracyJson, _ := json.Marshal(accuracy_obj)
    //ioutil.WriteFile("/home/mpiuser/Documents/FYP/gdelt/rf.json", accuracyJson, 0644)
    //ioutil.WriteFile("/home/amanda/FYP/gdelt/rf.json", accuracyJson, 0644)
		ioutil.WriteFile(accuracyJsonFile, accuracyJson, 0644)
    fmt.Println(string(accuracyJson))
}


//========================functions for kmeans===================
func simplePythonCall(progName string, itr string) string{
	cmd := exec.Command("python3", progName, itr)
	out, err := cmd.CombinedOutput()
    	if err != nil { fmt.Println(err); }
    	//fmt.Println(reflect.TypeOf(out))
	return string(out)
}

func miningPythonCall(progName string, workflowNumber string, itr string) string{
	cmd := exec.Command("python3", progName, workflowNumber, itr)
	out, err := cmd.CombinedOutput()
	log.Println(cmd.Run())

	if err != nil {
		fmt.Println(err)
		// Exit with status 3.
    		os.Exit(3)
	}
	//fmt.Println(string(out))
	//check if msg is legit
	msg := string(out)
	//msg := ("Module Completed: " + progName)
	return msg
}

func removeIt(ss Accuracy_class, ssSlice []Accuracy_class) []Accuracy_class {
    for idx, v := range ssSlice {
        if v == ss {
            return append(ssSlice[0:idx], ssSlice[idx+1:]...)
        }
    }
    return ssSlice
}

type Accuracy_class struct {
    Clusters int64 `json:"Clusters"`
    Accuracy float64 `json:"Accuracy"`
}

func FindMaxAccuracy(Accuracy_set []Accuracy_class) (max Accuracy_class) {

	max = Accuracy_set[0]
	for _, accuracy_obj := range Accuracy_set {
		if accuracy_obj.Accuracy > max.Accuracy {
			max = accuracy_obj
		}
	}
	return max
}

func Display(accuracy_obj Accuracy_class){
	fmt.Println("No of clusters: ", accuracy_obj.Clusters)
	fmt.Println("Accuracy: ", accuracy_obj.Accuracy)
}

func accuracySelection (inChannel chan <- string, workflowNumber int) {
	fmt.Println("Accuracy selection for kmeans started")
	var files []string
	cmd := exec.Command("python", "-c", "from workflow import userScript; print userScript.outputLocation" + strconv.Itoa(workflowNumber))
	out, err := cmd.CombinedOutput()

	if err != nil {
		fmt.Println(err)
		// Exit with status 3.
		os.Exit(3)
	} else if out == nil{
		os.Exit(3)
	}
	root := string(out)[:len(out)-1]  + "kmeans/"
	accuracyKmeansFile := string(out)[:len(out)-1] + "kmeans.txt"

	/*
	cmd := exec.Command("python", "-c", "from workflow import userScript; print userScript.outputLocation3")
	out, err0 := cmd.CombinedOutput()
	if err0 != nil {
		fmt.Println(err0)
		// Exit with status 3.
    		os.Exit(3)
	}

	path := string(out)
    	subFolder := fmt.Sprintf("%s%s", path, "kmeans/")
	*/
	//root := "/home/mpiuser/Documents/FYP/gdelt/kmeans/"
	//root := "/home/amanda/FYP/gdelt/kmeans/"
  err1 := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
  	files = append(files, path)
    return nil
  })
	fmt.Println(files)
  	if err1 != nil {
    	panic(err1)
    }

	var Accuracy_set []Accuracy_class

  for _, file := range files {
  	//if directory ignore
		fi, err2 := os.Stat(file)
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		var mode = fi.Mode();
		if mode.IsDir() == true {
			continue
		}

		csvFile, _ := os.Open(file)

    reader := csv.NewReader(bufio.NewReader(csvFile))

    for {
			line, error := reader.Read()
			if error == io.EOF {
				break
			} else if error != nil {
				log.Fatal(error)
			}

			var clusters int64
			var accuracy float64
			clusters, _ = strconv.ParseInt(line[0],10,0)
			accuracy, _ = strconv.ParseFloat(line[1],64)
			//fmt.Println(reflect.TypeOf(newc))
			Accuracy_set = append(Accuracy_set, Accuracy_class{
			Clusters: clusters,
			Accuracy: accuracy,
			})
		}

		//Accuracy_set = removeIt(Accuracy_class{"No_of_clusters", "Accuracy"}, Accuracy_set)
  }

	//accuracyJson, _ := json.Marshal(Accuracy_set)
	//fmt.Println(string(accuracyJson))
	//fmt.Println(Accuracy_set)
	var max = FindMaxAccuracy(Accuracy_set)
	var n = max.Clusters
	writeAccuracyFile(n, accuracyKmeansFile)
	Display(max)
	//fmt.Println(display)

	msg:= "Best accuracy selection for kmeans done"
	inChannel <- msg

}

func writeAccuracyFile(n int64, accuracyKmeansFile string) {
    //f, err := os.Create("/home/mpiuser/Documents/FYP/gdelt/kmeans.txt")
    f, err := os.Create(accuracyKmeansFile)
    if err != nil {
        fmt.Println(err)
        return
    }
    l, err := f.WriteString(strconv.FormatInt(n,10))
    if err != nil {
        fmt.Println(err)
        f.Close()
        return
    }
    fmt.Println(l, "bytes written successfully")
    err = f.Close()
    if err != nil {
        fmt.Println(err)
        return
    }
}


///=====================================================

func messagePassing(inChannel <- chan string, outChannel chan <- string ){
	msg := <- inChannel
	outChannel <- msg
}
func integrateMessagePassing(inChannel1 <- chan string, inChannel2 <- chan string, outChannel chan <- string ){
	msg1 := <- inChannel1
	msg2 := <- inChannel2
	outChannel <- msg1 + msg2
}

func numOfFiles(folder string) int{
    files,_ := ioutil.ReadDir(folder)
    return len(files)
}

//reads a file and returns an array of comments beginning with ##
func readLines( progName string) [30]string{
		var commandsArray [30]string
    file, err := os.Open(progName)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    i := 0
    for scanner.Scan() {
        command := scanner.Text()
				//fmt.Println(len(command))
				if len(command) >1{
					//fmt.Println("dh")
					if command[0:2] == "##" {
						//fmt.Println(command[2:len(command)])
						commandsArray[i] = command[2:len(command)]
						i++
					}
				}
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

		return commandsArray
}

func main(){
	simplePythonCall1("logo.py")
	for i := 1; i<=3; i++{
		//check if input location is available
		fmt.Println((i))
		//c1 := "python -c from workflow import userScript; print userScript.inputDataset" + strconv.Itoa(i)
		cmd := exec.Command("python", "-c", "from workflow import userScript; print userScript.inputDataset" + strconv.Itoa(i))
		//cmd := exec.Command(c1)
		out, err := cmd.CombinedOutput()

		if err != nil {
			fmt.Println(err)
			// Exit with status 3.
	    os.Exit(3)
		} else if out == nil{
			os.Exit(3)
		} else {
			//input dataset from disk
			//check if empty
			inputDataset := string(out)[:len(out)-1]
			fmt.Print(inputDataset+"\n")
		}


		//check if output location is available
		cmd1 := exec.Command("python", "-c", "from workflow import userScript; print userScript.outputLocation" + strconv.Itoa(i))
		out1, err1 := cmd1.CombinedOutput()

		if err1 != nil {
			fmt.Println(err1)
			// Exit with status 3.
			os.Exit(3)
		} else if out1 == nil{
			os.Exit(3)
		} else {
			//output dataset from disk
			//check if empty
			outputDataset := string(out1)[:len(out1)-1]
			fmt.Print(outputDataset+"\n")
		}
	}

	commandsArray := readLines("workflow/userScript.py")
	fmt.Println(commandsArray)

	//configurations
	//simplePythonCall1("workflow/parslConfig.py")

	start := time.Now()
/*
	//start module execution from here onwards
	inChannelModule0 := make(chan string, 1)
	outChannelModule0 := make(chan string, 1)
	go pythonCall("workflow/"+commandsArray[0], inChannelModule0,"1")
	//pythonCall("workflow/gdeltFileSelection/dataFilesIntegration.py", inChannelModule1)
	go messagePassing(inChannelModule0, outChannelModule0)
	fmt.Println(<-outChannelModule0)

	outChannelModule1 := make(chan string, 1)
	go pythonCall("workflow/"+commandsArray[1], outChannelModule0,"1")
	//pythonCall("workflow/gdeltFileSelection/countrySelection.py", inChannelModule1)
	go messagePassing(outChannelModule0, outChannelModule1)
	fmt.Println(<-outChannelModule1)

	outChannelModule2 := make(chan string, 1)
	go pythonCall("workflow/"+commandsArray[2], outChannelModule1, "1")
	//pythonCall("workflow/selection/selectUserDefinedColumns.py", outChannelModule1)
	go messagePassing(outChannelModule1, outChannelModule2)
	fmt.Println(<- outChannelModule2)

	outChannelModule3 := make(chan string, 1)
	go pythonCall("workflow/"+commandsArray[3], outChannelModule2, "1")
	//pythonCall("workflow/cleaning/dropUniqueColumns.py", outChannelModule2)
	go messagePassing(outChannelModule2, outChannelModule3)
	fmt.Println(<- outChannelModule3)

	outChannelModule4 := make(chan string, 1)
	go pythonCall("workflow/"+commandsArray[4], outChannelModule3, "1")
	//pythonCall("workflow/cleaning/dropColumnsCriteria.py", outChannelModule2)
	go messagePassing(outChannelModule3, outChannelModule4)
	fmt.Println(<- outChannelModule3)

	outChannelModule5 := make(chan string, 1)
	//pythonCall("workflow/cleaning/dropRowsCriteria.py", outChannelModule3)
	go pythonCall("workflow/"+commandsArray[5], outChannelModule4, "1")
	go messagePassing(outChannelModule4, outChannelModule5)
	fmt.Println(<- outChannelModule5)

	outChannelModule6 := make(chan string, 1)
	go pythonCall("workflow/"+commandsArray[6], outChannelModule5, "1")
	//pythonCall("workflow/cleaning/removeDuplicateRows.py", outChannelModule4)
	go messagePassing(outChannelModule5, outChannelModule6)
	fmt.Println(<- outChannelModule6)

	outChannelModule7 := make(chan string, 1)
	//pythonCall("workflow/cleaning/missingValuesMode.py", outChannelModule5)
	go pythonCall("workflow/"+commandsArray[7], outChannelModule6, "1")
	go messagePassing(outChannelModule6, outChannelModule7)
	fmt.Println(<- outChannelModule7)

	outChannelModule8 := make(chan string, 1)
	//pythonCall("workflow/transformation/combineColumns.py", outChannelModule8)
	go pythonCall("workflow/"+commandsArray[9], outChannelModule7, "1")
	go messagePassing(outChannelModule7, outChannelModule8)
	fmt.Println(<- outChannelModule8)


	inChannelModule21 := make(chan string, 1)
	outChannelModule21 := make(chan string, 1)
	pythonCall("workflow/"+commandsArray[2], inChannelModule21,"2")
	//pythonCall("workflow/selection/selectUserDefinedColumns.py", inChannelModule1)
	messagePassing(inChannelModule21, outChannelModule21)
	fmt.Println(<-outChannelModule21)

	outChannelModule22 := make(chan string, 1)
	pythonCall("workflow/"+commandsArray[3], outChannelModule21,"2")
	//pythonCall("workflow/selection/dropUniqueColumns.py", inChannelModule1)
	messagePassing(outChannelModule21, outChannelModule22)
	fmt.Println(<-outChannelModule22)

	outChannelModule23 := make(chan string, 1)
	pythonCall("workflow/"+commandsArray[6], outChannelModule22, "2")
	//pythonCall("workflow/cleaning/removeDuplicateRows.py", outChannelModule4)
	messagePassing(outChannelModule22, outChannelModule23)
	fmt.Println(<- outChannelModule23)

	outChannelModule24 := make(chan string, 1)
	//pythonCall("workflow/cleaning/missingValuesMode.py", outChannelModule5)
	pythonCall("workflow/"+commandsArray[7], outChannelModule23, "2")
	messagePassing(outChannelModule23, outChannelModule24)
	fmt.Println(<- outChannelModule24)

	outChannelModule25 := make(chan string, 1)
	//pythonCall("workflow/integrateLabels/addLabelColumn.py", outChannelModule5)
	pythonCall("workflow/"+commandsArray[10], outChannelModule24, "2")
	messagePassing(outChannelModule24, outChannelModule25)
	fmt.Println(<- outChannelModule25)

	outChannelModule26 := make(chan string, 1)
	//pythonCall("workflow/integrateLabels/assignCountryCode.py", outChannelModule5)
	pythonCall("workflow/"+commandsArray[11], outChannelModule25, "2")
	messagePassing(outChannelModule25, outChannelModule26)
	fmt.Println(<- outChannelModule26)

	outChannelModule27 := make(chan string, 1)
	//pythonCall("workflow/integrateLabels/splitDate.py", outChannelModule5)
	pythonCall("workflow/"+commandsArray[12], outChannelModule26, "2")
	messagePassing(outChannelModule26, outChannelModule27)
	fmt.Println(<- outChannelModule27)
/*

	outChannelModule9 := make(chan string, 1)
	//pythonCall("workflow/integrateLabels/integrate.py", outChannelModule5)
	integratePythonCall("workflow/"+commandsArray[14], outChannelModule27, outChannelModule8, "1")
	integrateMessagePassing(outChannelModule27, outChannelModule8, outChannelModule9)
	fmt.Println(<- outChannelModule9)
*/
/*
	outChannelModule10 := make(chan string, 1)
	pythonCall("workflow/"+commandsArray[8], outChannelModule27, "1")
	//pythonCall("workflow/transformation/normalize.py", outChannelModule6)
	messagePassing(outChannelModule27, outChannelModule10)
	fmt.Println(<- outChannelModule10)

	outChannelModule11 := make(chan string, 1)
	for i := 1;  i<=10; i++ {
		y := miningPythonCall("workflow/" +commandsArray[15], "1", strconv.Itoa(i))
		time.Sleep(5000 * time.Millisecond)
		fmt.Println(y)
	}
	accuracySelection_rf(outChannelModule10, 1)
	//pythonCall("workflow/mining/randomForestClassification.py", outChannelModule5)
	//miningPythonCall("workflow/"+commandsArray[15], outChannelModule10, "1", "2")
	messagePassing(outChannelModule10, outChannelModule11)
	fmt.Println(<- outChannelModule11)

	outChannelModule12 := make(chan string, 1)
	pythonCall("workflow/"+commandsArray[20], outChannelModule11, "1")
	//pythonCall("workflow/mining/knowledge_presentation_rf.py", outChannelModule6)
	messagePassing(outChannelModule11, outChannelModule12)
	fmt.Println(<- outChannelModule12)
*/
	//inChannelModule31 := make(chan string,1)
	outChannelModule31 := make(chan string, 1)
	//pythonCall("workflow/cleaning/dropUserDefinedColumns.py", outChannelModule5)
	go pythonCall("workflow/"+commandsArray[16], outChannelModule7, "3")
	go messagePassing(outChannelModule7, outChannelModule31)
	fmt.Println(<- outChannelModule31)

	outChannelModule32 := make(chan string, 1)
	for i := 1;  i<=10; i++ {
		x := miningPythonCall("workflow/" +commandsArray[17], "3", strconv.Itoa(i))
		time.Sleep(5000 * time.Millisecond)
		fmt.Println(x)
  }

	accuracySelection(outChannelModule31, 3)
	messagePassing(outChannelModule31, outChannelModule32)
	fmt.Println(<- outChannelModule32)

	outChannelModule33 := make(chan string, 1)
	//pythonCall("workflow/mining/knowledge_presentation.py", outChannelModule5)
	go pythonCall("workflow/"+commandsArray[18], outChannelModule32, "3")
	go messagePassing(outChannelModule32, outChannelModule33)
	fmt.Println(<- outChannelModule33)

	outChannelModule34 := make(chan string, 1)
	//pythonCall("workflow/mining/svm.py", outChannelModule5)
	go pythonCall("workflow/"+commandsArray[19], outChannelModule33, "3")
	//fmt.Println("svm started")
	go messagePassing(outChannelModule33, outChannelModule34)
	fmt.Println(<- outChannelModule34)

	end := time.Now()

	duration := end.Sub(start)

	fmt.Println("\nDuration: " + duration.String())

}

