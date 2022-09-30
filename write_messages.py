from jinja2 import Environment, FileSystemLoader
import os
import json
filejson = open('env/env.json')
student = json.load(filejson)

environment = Environment(loader=FileSystemLoader("configs/"))

for filename in os.listdir("configs/"):
  template = environment.get_template(filename)

  content = template.render(student)

  with open("configs_out/"+filename, mode="w", encoding="utf-8") as message:
      message.write(content)
      print(f"... wrote {filename}")     



# from jinja2 import Environment, FileSystemLoader
# import os

# max_score = 100
# test_name = "Python Challenge"
# student = {"name": '"Sandrine"',  "score": 100}

# environment = Environment(loader=FileSystemLoader("templ"))

# for filename in os.listdir("templ"):
#   template = environment.get_template(filename)

#   #filenamed = f"message_{student['name'].lower()}.json"
#   content = template.render(
#       student,
#       max_score=max_score)

#   with open("out/"+filename, mode="w", encoding="utf-8") as message:
#       message.write(content)
#       print(f"... wrote {filename}")




 