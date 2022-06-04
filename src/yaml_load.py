#%%
import yaml

with open("config.yaml", 'r') as f:
    valuesYaml = yaml.load(f, Loader=yaml.FullLoader)

print(valuesYaml['global']['url_canonica'])

# %%
