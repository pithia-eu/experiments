cp config jupyter-u1:/home/jovyan/u1 -c notebook

import pykube
kube = pykube.HTTPClient(pykube.KubeConfig.from_file("config"))

obj = {
  "apiVersion": "batch/v1",
  "kind": "Job",
  "metadata": {
    "name": "pi"
  },
  "spec": {
    "template": {
      "spec": {
        "containers": [
          {
            "name": "pi",
            "image": "perl",
            "command": [
              "perl",
              "-Mbignum=bpi",
              "-wle",
              "print bpi(2000)"
            ]
          }
        ],
        "restartPolicy": "Never"
      }
    },
    "backoffLimit": 4
  }
}

pykube.Job(kube, obj).delete()

pykube.Job(kube, obj).create()

pykube.Job(kube, obj).exists()

pods = pykube.Pod.objects(kube).filter(selector={"job-name":"pi"})

for pod in pods:
    print(pod)
    print(pod.logs())