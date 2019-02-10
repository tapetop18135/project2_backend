import threading
import time
def loop():
    t = True
    array = []
    start_time = time.time()
    for i in range(0,100):
        array.append(i)
        for j in range(0,12):
            temp = {}
            for k in range(0,73):
                for l in range(0,1440):            
                    if(t):
                        t = False
                    else:
                        t = True
                        
    print("Success")
    print("time is ",time.time() - start_time)


# threading.Thread(target=loop).start()
loop()

