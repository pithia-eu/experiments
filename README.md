# pithia

# Pithia reference architecture.
# Applications:
#               1. JupyterHub     (lines  x-x)
#               2. Apache Hadoop  (lines  x-x)
#               3. SMARTEST       (lines  x-x)
# Supplementary tools:
#               1. Kube-state-metrics  (lines  x-x)


 type: list
        description: pre-define alerts for VM CPU
        default:
        - alert: jhub_overloaded
          expr: '(count(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}  == 0) * {{PODS_PER_NODE_AVG}}) - count(kube_pod_labels{label_app="jupyterhub"}[1m]) < {{SCALE_UP_IF_FREE_SLOTS_ARE_LESS_THAN}}'
          for: 1m
        - alert: jhub_underloaded
          expr: '(count(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}  == 0) * {{PODS_PER_NODE_AVG}}) - count(kube_pod_labels{label_app="jupyterhub"}[1m]) > {{SCALE_DOWN_IF_FREE_SLOTS_ARE_LESS_THAN}}'
          for: 1m
        - alert: jhub_node_unschedulable
          expr: 'count(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}) > 0'
        required: true


kube_node_status_condition{condition="Ready",status="true",node=~"jupyterhub-node.*"} == 1 OR on() vector(0)


count(max_over_time(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}[30s]) == 1) OR on() vector(0)


kubectl cordon jupyterhub-node-172-31-32-95
kubectl cordon jupyterhub-node-172-31-36-149
kubectl cordon jupyterhub-node-172-31-36-214
kubectl cordon jupyterhub-node-172-31-37-1
kubectl cordon jupyterhub-node-172-31-44-112
kubectl delete pod jupyter-u10
kubectl delete pod jupyter-u11
kubectl delete pod jupyter-u12
kubectl delete pod jupyter-u4
kubectl delete pod jupyter-u5

sche
count(sum(kube_node_status_condition{condition="Ready",status="true",node=~"jupyterhub-node.*"} == 1) by (node) and sum(max_over_time(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}[30s]) == 0) by (node)) OR on() vector(0)

unsch
count(sum(kube_node_status_condition{condition="Ready",status="true",node=~"jupyterhub-node.*"} == 1) by (node) and sum(max_over_time(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}[30s]) == 1) by (node)) OR on() vector(0)

pods on sched
count(sum(max_over_time(kube_node_spec_unschedulable{node=~"jupyterhub-node.*"}[30s]) == 0) by (node) + sum(kube_pod_info{node=~"jupyterhub-node.*",pod=~"jupyter.*"}) by (node)) OR on() vector(0)

sum(max_over_time(kube_pod_info{node=~"jupyterhub-node.*",pod=~"jupyter.*"}[10s]) == 1) OR on() vector(0)