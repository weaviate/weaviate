from weaviate import Client
from uuid import uuid1

class TestConsecutiveCreateAndUpdate:
    client: Client
    img = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAABhGlDQ1BJQ0MgcHJvZmlsZQAAKJF9kT1Iw0AcxV/TSou0ONhBxCFD62RBVMRRq1CECqFWaNXB5NIvaNKSpLg4Cq4FBz8Wqw4uzro6uAqC4AeIm5uToouU+L+k0CLGg+N+vLv3uHsHCK0q08zAOKDplpFJJcVcflUMviKACEKIwy8zsz4nSWl4jq97+Ph6l+BZ3uf+HBG1YDLAJxLPsrphEW8QT29adc77xFFWllXic+Ixgy5I/Mh1xeU3ziWHBZ4ZNbKZeeIosVjqYaWHWdnQiKeIY6qmU76Qc1nlvMVZqzZY5578heGCvrLMdZojSGERS5AgQkEDFVRhIUGrToqJDO0nPfzDjl8il0KuChg5FlCDBtnxg//B727N4uSEmxROAn0vtv0RB4K7QLtp29/Htt0+AfzPwJXe9ddawMwn6c2uFjsCBraBi+uupuwBlzvA0FNdNmRH8tMUikXg/Yy+KQ8M3gL9a25vnX2cPgBZ6ip9AxwcAqMlyl73eHeot7d/z3T6+wEPO3J/B8olWgAAAAlwSFlzAAAuIwAALiMBeKU/dgAAAAd0SU1FB+UEDQgmFS2naPsAAAAZdEVYdENvbW1lbnQAQ3JlYXRlZCB3aXRoIEdJTVBXgQ4XAAAADElEQVQI12NgYGAAAAAEAAEnNCcKAAAAAElFTkSuQmCC"
    img2 = "/9j/4AAQSkZJRgABAQEASABIAAD/4QpKRXhpZgAASUkqAAgAAAAGABoBBQABAAAAVgAAABsBBQABAAAAXgAAACgBAwABAAAAAgAAADEBAgANAAAAZgAAADIBAgAUAAAAdAAAAGmHBAABAAAAiAAAAJoAAABIAAAAAQAAAEgAAAABAAAAR0lNUCAyLjEwLjE0AAAyMDIxOjAzOjI1IDE2OjI5OjQ3AAEAAaADAAEAAAABAAAAAAAAAAgAAAEEAAEAAAAAAQAAAQEEAAEAAADXAAAAAgEDAAMAAAAAAQAAAwEDAAEAAAAGAAAABgEDAAEAAAAGAAAAFQEDAAEAAAADAAAAAQIEAAEAAAAGAQAAAgIEAAEAAAA7CQAAAAAAAAgACAAIAP/Y/+AAEEpGSUYAAQEAAAEAAQAA/9sAQwAIBgYHBgUIBwcHCQkICgwUDQwLCwwZEhMPFB0aHx4dGhwcICQuJyAiLCMcHCg3KSwwMTQ0NB8nOT04MjwuMzQy/9sAQwEJCQkMCwwYDQ0YMiEcITIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIy/8AAEQgA1wEAAwEiAAIRAQMRAf/EAB8AAAEFAQEBAQEBAAAAAAAAAAABAgMEBQYHCAkKC//EALUQAAIBAwMCBAMFBQQEAAABfQECAwAEEQUSITFBBhNRYQcicRQygZGhCCNCscEVUtHwJDNicoIJChYXGBkaJSYnKCkqNDU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6g4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2drh4uPk5ebn6Onq8fLz9PX29/j5+v/EAB8BAAMBAQEBAQEBAQEAAAAAAAABAgMEBQYHCAkKC//EALURAAIBAgQEAwQHBQQEAAECdwABAgMRBAUhMQYSQVEHYXETIjKBCBRCkaGxwQkjM1LwFWJy0QoWJDThJfEXGBkaJicoKSo1Njc4OTpDREVGR0hJSlNUVVZXWFlaY2RlZmdoaWpzdHV2d3h5eoKDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uLj5OXm5+jp6vLz9PX29/j5+v/aAAwDAQACEQMRAD8A9/ooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAK+YP+GjvGH/QN0P8A78Tf/Ha+n6+AKAPYP+GjvGH/AEDdD/78Tf8Ax2j/AIaO8Yf9A3Q/+/E3/wAdrx+igD2D/ho7xh/0DdD/AO/E3/x2j/ho7xh/0DdD/wC/E3/x2vH6KAPYP+GjvGH/AEDdD/78Tf8Ax2j/AIaO8Yf9A3Q/+/E3/wAdrx+igD2D/ho7xh/0DdD/AO/E3/x2j/ho7xh/0DdD/wC/E3/x2vH6KAPYP+GjvGH/AEDdD/78Tf8Ax2j/AIaO8Yf9A3Q/+/E3/wAdrx+igD2D/ho7xh/0DdD/AO/E3/x2j/ho7xh/0DdD/wC/E3/x2vH6KAPYP+GjvGH/AEDdD/78Tf8Ax2j/AIaO8Yf9A3Q/+/E3/wAdrx+igD2D/ho7xh/0DdD/AO/E3/x2j/ho7xh/0DdD/wC/E3/x2vH6KAPYP+GjvGH/AEDdD/78Tf8Ax2j/AIaO8Yf9A3Q/+/E3/wAdrx+igD2D/ho7xh/0DdD/AO/E3/x2j/ho7xh/0DdD/wC/E3/x2vH6KAPYP+GjvGH/AEDdD/78Tf8Ax2j/AIaO8Yf9A3Q/+/E3/wAdrx+igD7/AKKKKACiiigAooooAKKKKACvgCvv+vgCgAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKAPv8AooooAKKKKACiiigAooooAK+AK+/6+AKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooA+/wCiiigAooooAKKKKACiiigAr4Ar7/r4AoAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigAooooAKKKKACiiigD7/AKKKKACiiigAooooAKKKKACvgCvv+vgC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"

    def __init__(self, client):
        self.client = client

    def batch_callback_result(self, results: dict) -> int:
        """
        Check batch results for errors and return the number of occurred errors.

        Parameters
        ----------
        results : dict
            The Weaviate batch creation return value.
        """

        if results is not None:
            for result in results:
                if 'result' in result and 'errors' in result['result']:
                    if 'error' in result['result']['errors']:
                        print(f"error: {result['result']['errors']}")
                        raise Exception("Some batch items failed!")

    def deleteTestClass(self, schemas, cls_name):
        if self.client.schema.contains(schemas):
            self.client.schema.delete_class(cls_name)

    def checkIfObjectsExist(self, uuids):
        for _id in uuids:
          # assert self.client.data_object.exists(_id)
          resp = self.client.data_object.get_by_id(_id, with_vector=True)
          if resp is None:
            print(f"ERROR!!! Object with ID: {_id} doesn't exist!!!")
            raise

    def consecutive_create_and_update_operations(self):
        print("Test started")

        cls_name = 'Test123'

        schemas = {
            'classes': [
                {
                    'class': cls_name,
                    "vectorizer": "none",
                    'vectorIndexConfig': {'skip': False},
                    'properties': [
                        {
                            'dataType': ['blob'],
                            'name': 'a',
                            'indexInverted': False,
                        }
                    ],
                },
            ]
        }

        self.deleteTestClass(schemas, cls_name)

        uuids = [str(uuid1()) for _ in range(28000)]
        assert len(list(set(uuids))) == len(uuids), 'uuids contain duplicates'

        # extend
        print(f"Create objects in batch of 50 items...")
        with self.client.batch(batch_size=50, callback=self.batch_callback_result) as batch:
            for _id in uuids:
                batch.add_data_object(data_object={'a': self.img}, class_name=cls_name, uuid=_id)
        self.client.batch.flush()

        print(f"Update objects with vector started...")
        x = 1
        # embed
        for _id in uuids:
            self.client.batch.add_data_object(data_object={'a': self.img2}, class_name=cls_name, uuid=_id, vector=[3,2,1])
            if x % 1000 == 0:
                print(f"updated {x} objects...")
            x += 1

        print("Check if objects exist...")
        # check
        self.checkIfObjectsExist(uuids)

        print(f"Update objects with new vector in batch of 50 items...")
        x = 1
        # update vectors
        with self.client.batch(batch_size=50, callback=self.batch_callback_result) as batch:
            for _id in uuids:
                batch.add_data_object(data_object={'a': self.img}, class_name=cls_name, uuid=_id, vector=[1,2,3])
                if x % 1000 == 0:
                    print(f"updated {x} objects...")
                x += 1
        self.client.batch.flush()

        print("Check if objects exist...")
        # check
        self.checkIfObjectsExist(uuids)

        self.deleteTestClass(schemas, cls_name)
        print("Test done")

c = Client('http://localhost:8080')

test = TestConsecutiveCreateAndUpdate(c)
test.consecutive_create_and_update_operations()