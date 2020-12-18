import json
import boto3
import os
import re

def getJobResults(jobId):

    pages = []

    textract = boto3.client('textract')
    response = textract.get_document_text_detection(JobId=jobId)
    
    pages.append(response)

    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):

        response = textract.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def campusConvert(campus):
      switcher = {
        "hm": "hmc",
        "sc": "sc",
        "po": "po",
        "pz": "pz",
        "cm": "cmc"
      }
      return switcher.get(campus, "hmc")

def shift(arr):
      # Skip first element
      for i in range(1, len(arr) - 1):
        arr[i] = arr[i+1]
      arr.pop()
      return arr

def getCoursesFromText(textArr):
    output = []
    
    curTerm = None
    curCount = 0
    curTitle = ''
    titleStart = False
    titleSplit = False
    for line in textArr:
        segments = line.split()

        # Keep track of how many lines after start of a title 
        # Necessary for getting split titles and credits of a course
        if titleStart:
            curCount += 1

        # If line starts with Year Term "Term" (2018 Fall Term), marks start of new semester
        # Apperance of "Fall" marks new year
        if len(segments) >= 3:
            yearMatch = re.match(r'[2-3][0-9]{3}', segments[0])
            termMatch = segments[2] == "Term"
            if yearMatch and termMatch:
                curTerm = segments[1]
                if curTerm == "Fall":
                    output.append({"Fall":[], "Spring":[], "Summer":[]})
    
        # Course title has Code Section (BIOL023 HM-03)
        if len(segments) >= 2:
            # Special cases: PE and HSA (maybe others) don't have length 4 code and have space between code
            if len(segments) >= 3 and len(segments[0]) < 4:
                segments[0] = segments[0] + segments[1]
                segments = shift(segments)
            # 0 may be interepted as O
            if len(segments[0]) >= 3 and segments[0][-3] == "O":
                segments[0] = segments[0][:-3] + "0" + segments[0][-2:]
    
            # Campus section number may be cut off (PZ-)
            campusMatch = re.match(r'[A-Z]{2}-', segments[1])
            codeMatch = re.match(r'([A-Z]{4}|[A-Z]{3}|[A-Z]{2})[0-9]{2}', segments[0])
            if campusMatch and codeMatch:
                titleStart = True
                titleSplit = False
                curCount = 0
                curTitle = ""
                dic = {"campus": campusConvert(segments[1][:2].lower()), "code": segments[0], "title": "Unparsable", "credits": 0.0}
                output[-1][curTerm].append(dic)
                if len(segments) >= 3:
                    titleSplit = True
                    curTitle = " ".join(segments[2:])
    
        if titleStart:
            # Actual title split into 1 and 9 lines after OR 0 and 8 lines after if title split up
            if (not titleSplit and (curCount == 1 or curCount == 9)) or (titleSplit and curCount == 8):
                # Date may be accidentally added so check that segments does not match a date
                dateMatch = re.match(r'[0-9]{2}/[0-9]{2}/[0-9]{4}', segments[0])
                append = " ".join(segments) if not dateMatch else ""
                curTitle += " " + append if (curTitle and append) else append
                output[-1][curTerm][-1]["title"] = curTitle
        
            # Credits either 4 OR 3 lines after if title split up
            if (not titleSplit and curCount == 4) or (titleSplit and curCount == 3):
                # Check that code has proper format
                creditsMatch = re.match(r'[0-4].[0-9]{3}', segments[0])
                if creditsMatch:
                    credits = float(segments[0])
                    output[-1][curTerm][-1]["credits"] = credits
        
    return output
    

def lambda_handler(event, context):
    notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    
    pdfTextExtractionStatus = json.loads(notificationMessage)['Status']
    pdfTextExtractionJobTag = json.loads(notificationMessage)['JobTag']
    pdfTextExtractionJobId = json.loads(notificationMessage)['JobId']
    pdfTextExtractionDocumentLocation = json.loads(notificationMessage)['DocumentLocation']
    
    pdfTextExtractionS3ObjectName = json.loads(json.dumps(pdfTextExtractionDocumentLocation))['S3ObjectName']
    pdfTextExtractionS3Bucket = json.loads(json.dumps(pdfTextExtractionDocumentLocation))['S3Bucket']
    
    print(pdfTextExtractionJobTag + ' : ' + pdfTextExtractionStatus)
    
    pdfTextArr = []
    
    if(pdfTextExtractionStatus == 'SUCCEEDED'):
        response = getJobResults(pdfTextExtractionJobId)
        
        for resultPage in response:
            for item in resultPage["Blocks"]:
                if item["BlockType"] == "LINE":
                    pdfTextArr.append(item["Text"])
                    
        s3 = boto3.client('s3')
        
        courses = getCoursesFromText(pdfTextArr)
        
        fileName = os.path.splitext(pdfTextExtractionS3ObjectName)[0]
        
        # Delete original pdf
        pdfFileName = fileName + '.pdf'
        s3.delete_object(Bucket=pdfTextExtractionS3Bucket, Key=pdfFileName)
        
        # Write json back to s3
        outputTextFileName = fileName + '.json'
        s3.put_object(Body=bytes(json.dumps(courses).encode('UTF-8')), Bucket=pdfTextExtractionS3Bucket, Key=outputTextFileName)