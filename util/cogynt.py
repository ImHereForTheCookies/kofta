from collections import defaultdict


class TracePath(object):
    def __init__(self, template_solution_events: list, starting_key='id', max_depth=10):
        # Create dictionary where each key is pattern id and the values are published_by ids
        self.max_depth = max_depth
        self.input_mappings = defaultdict(lambda: set())
        self.event_mappings = defaultdict(lambda: set())
        for message in template_solution_events:
            # Assumes key error only for published_by
            try:
                self.input_mappings[message['event'][starting_key]].add(message['event']['published_by'])
                self.event_mappings[message['id']].add(message['event']['published_by'])
            except KeyError:
                self.input_mappings[message['event'][starting_key]].add("Root Node")
                self.event_mappings[message['id']].add("Root Node")

    def trace_path(self, search_id: int, depth=1):
        if depth == 1:
            if len(self.input_mappings[search_id]) == 0:
                return "Node has no parents"
            return {search_id: {publish_id: self.trace_path(publish_id, depth=depth+1) for publish_id in self.input_mappings[search_id]
                                if search_id != publish_id}}  # Avoid recursion case
        # Case for maximum recursion depth
        if depth > self.max_depth:
            return f"Max depth ({self.max_depth})."

        # Case when you reach an event has no published by (assigned "Root Node.")
        if search_id == "Root Node":
            return search_id

        # Case when parent does not have provence enabled
        if search_id not in self.event_mappings:
            return {search_id: "Maximum provenance depth)"}

        # Needs to return dict comprehension to yield nested dicts AFAIK
        return {publish_id: self.trace_path(publish_id, depth=depth+1) for publish_id in self.event_mappings[search_id]
                if search_id != publish_id}  # Avoid recursion case

test = TracePath(template_solution_events)
test.trace_path('35691264-3151-7666-3e65-86a4a2156529')