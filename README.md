### Architecture overview

We've implemented an asynchronous, choreographed SAGA pattern with eventual consistency. Services react to messages via RabbitMQ and perform local transactions that atomically persist both state changes and event records (the outbox pattern). To support ACID guarantees, we've migrated to PostgreSQL. 

The events/messages are published using change data capture via Debezium Server, ensuring reliable delivery even during service failures. And our architecture relies on the guarantee for the delivery that comes from the message bus to ensure eventual consistency.

Each message handler follows this pattern (within a single transaction):
- Check if the message has already been processed. If yes, acknowledge and return.
- Execute business logic and persist an event marking the action taken (or compensation step).
- Acknowledge the message.

The system maintains eventual consistency despite container failures and has zero-downtime if one of the microservice containers has failed. To support that we have put a gateway that balances the traffic of HTTP requests and automatically stops routing requests to a faulty service. However, the system availability depends on the databases, message broker, CDCs, and gateway. On their failure, restarting the faulty container should return the system to full functionality and consistency.

The order service simulates synchronous behaviour by blocking the HTTP request until either a timeout occurs or an event confirms order success or failure. If the gateway or the order service handling the current checkout HTTP request fails, the checkout will still be completed (if there is enough money and stock), even though it is not possible to return a proper response to the HTTP request.

If the databases, message broker or CDCs are down, all events will be delayed until they are operational. All checkouts that have already started (marked as started in the databases) will be completed once they are up again.

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.
