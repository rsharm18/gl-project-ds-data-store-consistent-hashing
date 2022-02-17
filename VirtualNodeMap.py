import random
import math
from tokenize import String
from typing import List

# Stores the vnode to node mapping
# Composed within a node so that every node has its own vnode mapping


class VirtualNodeMap:
    def __init__(self, node_names, TOTAL_VIRTUAL_NODES):
        self._vnode_map = {}
        self._node_names = node_names
        self._TOTAL_VIRTUAL_NODES = TOTAL_VIRTUAL_NODES
        self._node_vnode_map = {}

    @property
    def vnode_map(self):
        return self._vnode_map

    @property
    def node_names(self):
        return self._node_names

    # Populates the Virtual Node Nap, given the set of Node names.
    # Creates a mapping of Virtual Node to corresponding assigned physical Node
    def populate_map(self):

        # Problem statement 1
        # Generate a dict of vnode ids (0 to (TOTAL_VIRTUAL_NODES - 1) mapped randomly
        # but equally (as far as maths permits) to node names
        # pass

        # allocated_vm_count = 0
        # while(allocated_vm_count < self._TOTAL_VIRTUAL_NODES):
        #     self._vnode_map[allocated_vm_count] = random.choice(
        #         self._node_names)
        #     allocated_vm_count += 1
        self.distribute_vnodes_to_nodes()

        print("self._TOTAL_VIRTUAL_NODES ", self._TOTAL_VIRTUAL_NODES)
        print("# of nodes = ", len(self._node_names))

        self.prepare_node_to_vnNodes_lookup()

    # Return the vnode name mapped to a particular vnode

    def get_node_for_vnode(self, vnode):
        return self._vnode_map[vnode]

    # Returns the vnode name where a particular key is stored
    # It finds the vnode for the key through modulo mapping, and then looks up the physical node
    def get_assigned_node(self, key):
        vnode = key % self._TOTAL_VIRTUAL_NODES
        return self._vnode_map[vnode]

    # Assign a new node name as mapping for a particular vnode
    # This is useful when vnodes are remapped during node addition or removal
    def set_new_assigned_node(self, vnode, new_node_name):
        self._vnode_map[vnode] = new_node_name

    def prepare_node_to_vnNodes_lookup(self):

        for key, value in self._vnode_map.items():
            self._node_vnode_map.setdefault(value, []).append(key)

        for key, value in self._node_vnode_map.items():
            print(key, " # of vnodes ", len(value))

        # for key, value in node_vnode_map.items():
        #     print(key, " vnodes ", value)

    def distribute_vnodes_to_nodes(self) -> int:

        vNodes_to_allocate = self._TOTAL_VIRTUAL_NODES

        if(self._TOTAL_VIRTUAL_NODES % len(self._node_names) != 0):
            vNodes_to_allocate = vNodes_to_allocate - \
                (self._TOTAL_VIRTUAL_NODES % len(self._node_names))

        vnodes_per_node_count = math.floor(
            vNodes_to_allocate / len(self._node_names))

        print(" nodes not allocating are ",
              self._TOTAL_VIRTUAL_NODES - vNodes_to_allocate)

        allocated_vnode_count = 0
        node_count = 0

        random_nodes = []
        while(allocated_vnode_count < vNodes_to_allocate):
            self._vnode_map[allocated_vnode_count] = self.select_random_node(
                random_nodes)
            allocated_vnode_count += 1

            # reset random_nodes when the list is exhausted
            if(len(random_nodes) == len(self._node_names)):
                random_nodes.clear()

            if(allocated_vnode_count % vnodes_per_node_count == 0):
                node_count += 1

        random_nodes = []
        if(allocated_vnode_count != self._TOTAL_VIRTUAL_NODES):
            print("# of nodes yet to be allocated are ",
                  self._TOTAL_VIRTUAL_NODES - allocated_vnode_count)
            while(allocated_vnode_count < self._TOTAL_VIRTUAL_NODES):
                allocated_vnode_count += 1
                self._vnode_map[allocated_vnode_count] = self.select_random_node(
                    random_nodes)

        return allocated_vnode_count

    def select_random_node(self, selectedNodes: List) -> String:
        random_node = random.choice(self._node_names)

        if(random_node in selectedNodes):
            return self.select_random_node(selectedNodes)

        selectedNodes.append(random_node)
        return random_node

    def get_vnodes_for_node(self, node_name) -> List:
        return self._node_vnode_map[node_name]
