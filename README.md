

#k8s-events-hub

Kubernetes creates number of objects, Event is one of type of object in Kubernetes. On state change, success or failure ,
k8s controller creates event type of object. This events are not directly accessible by another application / services. 

k8s-events-hub helps to stream kubernetes events to kafka so that another services can take action on state change.


#TODO

1. Refactor code
2. Add testcases
3. Containerize application
