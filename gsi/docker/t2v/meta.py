from transformers import AutoConfig


class Meta:
    config: AutoConfig

    def __init__(self, model_path):
        self.config = AutoConfig.from_pretrained(model_path)

    def get(self):
        return {
            'model': self.config.to_dict()
        }

    def getModelType(self):
        return self.config.to_dict()['model_type']

    def get_architecture(self):
        architecture = None
        conf = self.config.to_dict()
        if "architectures" in conf:
            architecture = conf["architectures"][0]
        return architecture
