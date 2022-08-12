# Text Summarization Module

#### Schema

```json
schema = {
  "classes": [{
    "class": "Article",
    "description": "A written text, for example a news article or blog post",
    "properties": [
      {
        "dataType": [
          "string"
        ],
        "description": "Title of the article",
        "name": "title"
      },
      {
        "dataType": [
          "text"
        ],
        "description": "The content of the article",
        "name": "content"
      }
    ]
  }]
}
```

## Design

#### Query


```json
{
    Get {
      Article {
        content
        _additional {
      summary (
        properties: ["content"],
      ) {
            result     
          }
        }
      }
    }
  }
```

#### Result

```json
{
    "data": {
        "Get": {
            "Article": [
                {
                    "content": "The tower is 324 metres (1,063 ft) tall, about the same height as an 81-storey building, and the tallest structure in Paris. Its base is square, measuring 125 metres (410 ft) on each side. During its construction, the Eiffel Tower surpassed the Washington Monument to become the tallest man-made structure in the world, a title it held for 41 years until the Chrysler Building in New York City was finished in 1930. It was the first structure to reach a height of 300 metres. Due to the addition of a broadcasting aerial at the top of the tower in 1957, it is now taller than the Chrysler Building by 5.2 metres (17 ft). Excluding transmitters, the Eiffel Tower is the second tallest free-standing structure in France after the Millau Viaduct.",
                    "_additional": {
                        "sumamry": [
                          {
                              "result": "The Eiffel Tower is a landmark in Paris, France."
                          }
                      ]
                  }
                }
            ]
        }
    }
}

```
### Test results

#### Client package

![client_test](https://user-images.githubusercontent.com/58045206/165968874-70a4cbb4-cc04-44de-b661-965037bbfce3.png)


#### summary package
![summary_test](https://user-images.githubusercontent.com/58045206/165968881-9b82a62d-f267-404a-90e9-6f45296e9d4f.png)

