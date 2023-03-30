import time
import unittest
import requests


class SmokeTest(unittest.TestCase):
    def _waitForStartup(self):
        url = 'http://localhost:8080/.well-known/ready'

        for i in range(0, 100):
            try:
                res = requests.get(url)
                if res.status_code == 204:
                    print("Weaviate is working starting tests")
                    return
                else:
                    raise Exception(
                            "status code is {}".format(res.status_code))
            except Exception as e:
                print("Attempt {}: {}".format(i, e))
                time.sleep(2)

        raise Exception("did not start up")

    def testVectorizing(self):
        self._waitForStartup()
        url = 'http://localhost:8080/vectors/'
        req_body = {'text': '1999999'}

        res = requests.post(url, json=req_body)
        resBody = res.json()
        print(resBody)
        self.assertEqual(200, res.status_code)


if __name__ == "__main__":
    unittest.main()
