# This script is provided with no warranties or guarantees that it will work
# nor that any spelling is correct.

# It relies on having an esoteric program called 'dot' installed and 
# probably won't work if you aren't familiar with banging your head against the keyboard 
# for as long as it takes. It's really annoying to get working.  
# If you don't have 'dot' installed, it does use a different 
# component of networkx 

# As always, It is recommended that you not run scripts from random people you heard
# speak at a conference.

import networkx as nx
import matplotlib.pyplot as plt
import random
from enum import Enum
from datetime import time

DOT_INSTALLED = True

class NodeStatus(str, Enum):
    NOTREADY = 'red'
    WAITING = 'yellow'
    RUNNING = 'green'
    COMPLETE = 'blue'

class MyGraph:
    concurrent_tasks = 3

    def __init__(self, node_list, edge_list) -> None:
        self.G = nx.DiGraph()
        self.G.add_nodes_from(node_list)
        self.G.add_edges_from(edge_list)

        self.copy_queue = []
        self.spark_queue = []

        for edge in self.G.edges.data():
            self.G.nodes[edge[1]]['dep_count'] += 1
            edge[2]['cost'] = self.G.nodes[edge[0]]['duration']

        #self.enqueue_nodes()

    def enqueue_nodes(self):
        for node in self.G.nodes.data():
            if node[1]['dep_count'] == 0 and node[1]['color'] == NodeStatus.NOTREADY:
                node[1]['color'] = NodeStatus.WAITING
                match node[1]['queue']:
                    case 'copy':
                        self.copy_queue.append(node)
                    case 'spark':
                        self.spark_queue.append(node)

    def _get_filtered_queue(self, queue, status):
        return [node for node in queue if node[1]['color'] == status]

    def start_any_queue_tasks(self,queue):
        tasks_to_start = min((self.concurrent_tasks,len(queue))) - len(self._get_filtered_queue(queue, NodeStatus.RUNNING))
        if tasks_to_start <= 0:
            return
        
        for _ in range(tasks_to_start):
            strategy = random.randint(1,4)
            match strategy:
                case 1:
                    sorted(self._get_filtered_queue(queue, NodeStatus.WAITING), 
                        key=lambda s: s[1]['duration'], 
                        reverse=True)[0][1]['color'] = NodeStatus.RUNNING

                case 2:
                    sorted(self._get_filtered_queue(queue, NodeStatus.WAITING), 
                        key=lambda s: s[1]['duration'], 
                        reverse=False)[0][1]['color'] = NodeStatus.RUNNING
                
                case _:
                    sorted(self._get_filtered_queue(queue, NodeStatus.WAITING), 
                        key=lambda s: s[1]['duration'], 
                        reverse=True)[0][1]['color'] = NodeStatus.RUNNING
            
    def decrement_running_tasks(self):
        for node in self._get_filtered_queue(self.copy_queue,NodeStatus.RUNNING):
            node[1]['duration'] -= 1
        
        for node in self._get_filtered_queue(self.spark_queue,NodeStatus.RUNNING):
            node[1]['duration'] -= 1

    def _complete_task(self, queue, node):
        queue.remove(node)
        node[1]['color'] = NodeStatus.COMPLETE
        for edge in self.G.edges.data():
            if edge[0] == node[0]:
                edge[2]['IsComplete'] = True
                self.G.nodes[edge[1]]['dep_count'] -= 1

    def complete_tasks(self):
        for node in self._get_filtered_queue(self.copy_queue,NodeStatus.RUNNING):
            if node[1]['duration'] == 0:
                self._complete_task(self.copy_queue, node)
        
        for node in self._get_filtered_queue(self.spark_queue,NodeStatus.RUNNING):
            if node[1]['duration'] == 0:
                self._complete_task(self.spark_queue, node)

    def simulate(self):
        self.enqueue_nodes()
        self.start_any_queue_tasks(self.copy_queue)
        self.start_any_queue_tasks(self.spark_queue)
        self.decrement_running_tasks()
        self.complete_tasks()

        
    def draw(self,draw_labels = True):
        colors = [node[1]['color'] for node in self.G.nodes.data()]
        labels = {n[0]: f'{n[0]}: ({n[1]['duration']})' for n in self.G.nodes.data()}

        # {n: n[1]['duration'] for n in self.G.nodes.data()}
        plt.clf() # clear the graph before redrawing
        pos = nx.spring_layout(self.G)
        if (DOT_INSTALLED):
            pos = nx.nx_pydot.graphviz_layout(self.G, prog='dot')
        nx.draw_networkx_nodes(self.G, pos, cmap=plt.get_cmap('jet'), 
                            node_color = colors, node_size = 500)
        if (draw_labels):
            nx.draw_networkx_labels(self.G, pos,labels=labels)
        else:
            labels = {n[0]: f'{n[1]['duration']}' for n in self.G.nodes.data()}
            nx.draw_networkx_labels(self.G, pos,labels=labels)
        nx.draw_networkx_edges(self.G, pos, edgelist=[n for n in self.G.edges.data() if n[2]['IsComplete'] == False], edge_color='r', arrows=True)
        nx.draw_networkx_edges(self.G, pos, edgelist=[n for n in self.G.edges.data() if n[2]['IsComplete'] == True], arrows=True)
        plt.show()

def main():

    nodes = [
        ('ex_Cities',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_Countries',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_DeliveryMethods',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_People',{"duration":3,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_StateProvinces',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_Suppliers',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_BuyingGroups',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_Customers',{"duration":3,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_CustomerCategories',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_Colors',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_PackageTypes',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_StockItems',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_Orders',{"duration":5,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('ex_OrderLines' ,{"duration":7,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"copy"})
        ,('int_Cities',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_Countries',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_DeliveryMethods',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_People',{"duration":3,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_StateProvinces',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_Suppliers',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_BuyingGroups',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_Customers',{"duration":3,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_CustomerCategories',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_Colors',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_PackageTypes',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_StockItems',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_Orders',{"duration":5,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('int_OrderLines' ,{"duration":5,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('prep_dim_customer',{"duration":3,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('prep_dim_employee',{"duration":1,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('prep_dim_product',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('prepare_fact_sales',{"duration":5,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('load_dim_customer',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('load_dim_employee',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('load_dim_product',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('replace_fact_sk',{"duration":2,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
        ,('load_fact',{"duration":3,"color":NodeStatus.NOTREADY,"dep_count":0,"queue":"spark"})
    ]
    edges = [
        ('ex_Cities','int_Cities',{"IsComplete":False}),
        ('ex_Countries','int_Countries',{"IsComplete":False}),
        ('ex_DeliveryMethods','int_DeliveryMethods',{"IsComplete":False}),
        ('ex_People','int_People',{"IsComplete":False}),
        ('ex_StateProvinces','int_StateProvinces',{"IsComplete":False}),
        ('ex_Suppliers','int_Suppliers',{"IsComplete":False}),
        ('ex_BuyingGroups','int_BuyingGroups',{"IsComplete":False}),
        ('ex_Customers','int_Customers',{"IsComplete":False}),
        ('ex_CustomerCategories','int_CustomerCategories',{"IsComplete":False}),
        ('ex_Colors','int_Colors',{"IsComplete":False}),
        ('ex_PackageTypes','int_PackageTypes',{"IsComplete":False}),
        ('ex_StockItems','int_StockItems',{"IsComplete":False}),
        ('ex_Orders','int_Orders',{"IsComplete":False}),
        ('ex_OrderLines','int_OrderLines',{"IsComplete":False}),
        ('int_BuyingGroups','prep_dim_customer',{"IsComplete":False}),
        ('int_Cities','prep_dim_customer',{"IsComplete":False}),
        ('int_Countries','prep_dim_customer',{"IsComplete":False}),
        ('int_Customers','prep_dim_customer',{"IsComplete":False}),
        ('int_CustomerCategories','prep_dim_customer',{"IsComplete":False}),
        ('int_DeliveryMethods','prep_dim_customer',{"IsComplete":False}),
        ('int_People','prep_dim_customer',{"IsComplete":False}),
        ('int_StateProvinces','prep_dim_customer',{"IsComplete":False}),
        ('int_People','prep_dim_employee',{"IsComplete":False}),
        ('int_Colors','prep_dim_product',{"IsComplete":False}),
        ('int_PackageTypes','prep_dim_product',{"IsComplete":False}),
        ('int_StockItems','prep_dim_product',{"IsComplete":False}),
        ('int_Suppliers','prep_dim_product',{"IsComplete":False}),
        ('int_Orders','prepare_fact_sales',{"IsComplete":False}),
        ('int_OrderLines','prepare_fact_sales',{"IsComplete":False}),
        ('prep_dim_customer','load_dim_customer',{"IsComplete":False}),
        ('prep_dim_employee','load_dim_employee',{"IsComplete":False}),
        ('prep_dim_product','load_dim_product',{"IsComplete":False}),
        ('prepare_fact_sales','replace_fact_sk',{"IsComplete":False}),
        ('load_dim_customer','replace_fact_sk',{"IsComplete":False}),
        ('load_dim_employee','replace_fact_sk',{"IsComplete":False}),
        ('load_dim_product','replace_fact_sk',{"IsComplete":False}),
        ('replace_fact_sk','load_fact',{"IsComplete":False})
    ]

    G = MyGraph(nodes,edges)
    G.draw()
    total_iter = 0
    while len([n for n in G.G.nodes.data() if n[1]['color'] != NodeStatus.COMPLETE]) > 0:
        G.simulate()
        G.draw(draw_labels=False)
        total_iter += 1

    print(total_iter)

if __name__ == "__main__":
    main()
