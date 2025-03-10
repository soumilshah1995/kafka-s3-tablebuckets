try:
    from kafka import KafkaProducer
    from faker import Faker
    import json
    from time import sleep
    import uuid
except Exception as e:
    pass

producer = KafkaProducer(bootstrap_servers='localhost:9092')
_instance = Faker()

global faker

faker = Faker()


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [

            uuid.uuid4().__str__(),
            faker.name(),
            faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
            faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
            faker.random_int(min=10000, max=150000),
            faker.random_int(min=18, max=60),
            faker.random_int(min=0, max=100000),
            faker.unix_time()
        ]



for _ in range(100):

    for i in range(1,20):
        columns =  ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
        data_list = DataGenerator.get_data()
        json_data = dict(
            zip
            (columns,data_list

             )
        )
        _payload = json.dumps(json_data).encode("utf-8")
        response = producer.send('customers', _payload)
        print(_payload)
