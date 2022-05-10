### this is chicago intelligent strategy program
### use Nameko microservices 
## parts, including get data and generate report
## run step:
# 1. docker run -d -p 5672:5672 rabbitmq:3
# 2. nameko run main
# 3. nameko shell
# and you can run get data througn 
# 4. n.rpc.get_data_service.get__data()
# while you are running this, it could not stop getting data every 24 hours
# you can generate report, for example:
# 5. n.rpc.generate_report_airport.generate_report_airport_s()
# ...

