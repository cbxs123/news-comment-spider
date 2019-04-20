#-*-encoding=utf-8-*-
import os
while True:
    print('temp1.txt---源语言(pdf-复制-黏贴)；temp2.txt---目标语言(google.translate)；temp1.txt不输入则退出')
    open('temp1.txt', 'w+').close()
    os.system('notepad temp1.txt') #win
	#os.system('less temp1.txt')   #linux
    content1 = ' '.join([line.strip()+'##' if line.strip()[-1]=='.' else line.strip() for line in open('temp1.txt', "r")]).replace('- ', '').split('##')
    if len(content1[0]) > 0:
        f=open('temp2.txt','w+')
        list(map(lambda x:f.write(x+'\n'),content1))
        f.close()
        os.system('notepad temp2.txt')
		#os.system('less temp2.txt')  #linux
    else:
        break