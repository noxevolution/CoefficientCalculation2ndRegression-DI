#!/usr/bin/python

import csv, MySQLdb, traceback, sys, glob, os, collections
from collections import defaultdict

dirPath = "C:\\Python27\\CoefficientCalculation2ndRegression-DI"
inputCsvFileName = "generate_data_2ndregressionrun.csv"
#List of all csv files
#allCsvFilesList = glob.glob(dirPath+"*.csv")
#allCsvFilesList = list(os.listdir(dirPath))
dependentFile = "C:\\Python27\\CoefficientCalculation2ndRegression-DI\\dependent\\dependent_independent_vars.csv"

listCorsInputOutputFiles =  collections.defaultdict(list)
independentKeysOfDepenedentVar = collections.defaultdict(list)
'''
for csvfile in allCsvFilesList:
	csvfile = str(csvfile)
	if 'final_output_WOI_Forester_Total_regression' in csvfile:
	
		outputFileSbstr = csvfile.split('_')
		
		inputFile = 'ts_forester_'+outputFileSbstr[len(outputFileSbstr)-1][:-4]+'_import.csv'
		
		if os.path.isfile(inputFile):
			listCorsInputOutputFiles[csvfile] = inputFile
'''

def getInputFileName(outputFileName):
	if 'final_output_WOI_Forester_Total_regression' in outputFileName:
		outputFileSbstr = outputFileName.split('_')
		inputFileVar = 'ts_forester_'+outputFileSbstr[len(outputFileSbstr)-1][:-4]+'_import.csv'
		return inputFileVar
		

i_f_dependent = open(dependentFile)
dependentReader = csv.reader(i_f_dependent)
dependentReader.next()

keyVar 		= ''
inputFileVar 	= ''
d_var_file_name = ''
d_var_unique_id = ''
i_var_file_name	= ''
i_var_unique_id = ''

for i ,lin in enumerate(dependentReader):
	
	if lin[0] !='':
		d_var_file_name = lin[0]
	
	if lin[1] !='':
		d_var_unique_id = lin[1]
		keyVar = d_var_file_name.replace(' ','_')+'###'+d_var_unique_id.replace(' ','_')	
	
	if lin[2] !='':
		i_var_file_name = lin[2]
		
	if lin[3] !='':
		i_var_unique_id = lin[3]
	
	
	csvfile = str(lin[0])
	print csvfile
		
	listCorsInputOutputFiles[keyVar] = {'d_var_file_name':d_var_file_name,'d_var_unique_id':d_var_unique_id,'i_var_file_name':i_var_file_name}
	
	'''if listCorsInputOutputFiles[keyVar].has_key('i_var_unique_id')== False:
		listCorsInputOutputFiles[keyVar]['i_var_unique_id']  = []   '''
		
	#listCorsInputOutputFiles[keyVar]['i_var_unique_id'][0]  = "KKKK"  
	#listCorsInputOutputFiles[keyVar].setdefault('i_var_unique_id',[]).append( i_var_unique_id )
	#listCorsInputOutputFiles[keyVar].setdefault('i_var_unique_id',list()).append(i_var_unique_id)
	independentKeysOfDepenedentVar[keyVar].append(i_var_unique_id)


#print independentKeysOfDepenedentVar
#exit()

headerTop = ['uniqueid', 'forecasting','tool tip','node name','Category','delta','Dependent','08-2010','Monthly']

 

for key,value in listCorsInputOutputFiles.iteritems():
	inputCsvFileName = "generate_data_2ndregressionrun"+'_'+value['d_var_unique_id']+'.csv'
	ofile = open(inputCsvFileName, 'wb')
	writer = csv.writer(ofile)
	writer.writerow(headerTop)     
	
	print value
	
	variables = list()	
	var_outputFile 	= value['d_var_file_name']
	var_inputFile 	= value['i_var_file_name']
	flag = False
	i_f_out = open(var_outputFile)
	reader = csv.reader(i_f_out)
	header = reader.next()
	
	
	for index, line in enumerate(reader):
		if line[0] == value['d_var_unique_id']:
			variables.append(line[0])
			line.insert(6,'D1')
			writer.writerow(line)
			flag = True 
			break 
	
	if flag:
	
		listDependKeys = independentKeysOfDepenedentVar[key]
		print listDependKeys
		if 'ALL' in listDependKeys:
			getAll = True
		else:
			getAll = False
		
			
		i_f_input = open(var_inputFile)
		reader = csv.reader(i_f_input)
		header1 = reader.next()
		
		if getAll:
			
			for index, line in enumerate(reader):
				if var_outputFile == var_inputFile and line[0]== value['d_var_unique_id']:
					continue
				line.insert(6,'I1')
				writer.writerow(line)     
		else:
			
			for index, line in enumerate(reader):
				
				if var_outputFile == var_inputFile and line[0]== value['d_var_unique_id']:
					continue
				if line[0] in listDependKeys:
					print line[0]
					line.insert(6,'I1')
					writer.writerow(line)     
					
			
		
	 
	
	

