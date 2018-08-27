from itertools import islice
import re
import sys

def readBlock(inputData, rowValue):
    with open(inputData) as reader:
        while True:
            lines = list(islice(reader, rowValue))
            if lines:
                yield lines
            else:
                break

def fetchData(text):
    if text == "\n":
        return text
    return text.split(":")[1].strip()
 
# whole preprocess
if __name__ == "__main__":
    print(sys.argv[1])
    print(sys.argv[0])
    with open(sys.argv[2], "w") as writer:
        headerLine = "ProductId"+","+"UserId"+","+"ProfileName"+","+"Helpfulness"+","+"Score" +","+"Time" +","+ "Summary" +","+ "Text"+"\n"
        writer.write(headerLine)
        for sentences in readBlock(sys.argv[1], 9):
            out = ""
            for sentence in sentences:
                if "review/summary:" in sentence or "review/text:" in sentence or "review/profileName:" in sentence:
                    data = fetchData(sentence)
                    temp = re.sub("[^a-zA-Z]", " ", data)
                elif "product/productId:" in sentence or "review/userId:" in sentence or "review/score:" in sentence or "review/time:" in sentence or (sentence == "\n"):
                    temp = fetchData(sentence)
                elif "review/helpfulness:" in sentence:
                    temp = " "
                else:
                    continue
                if temp == "\n" :
                     out += temp
                else:
                    out += temp + ","
            writer.write(out)