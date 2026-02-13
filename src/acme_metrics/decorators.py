"""Domain-specific decorators for metrics computation stages.

Three stage decorators map to the metrics lifecycle:

- ``load``: Load source data for metrics computation.
- ``compute``: Calculate metrics from loaded data.
- ``save``: Write computed metrics to the metrics store.

When used inside an ``acme_metadeco.run()`` context, execution metadata
(timings, lineage, errors) is automatically tracked.
"""

from acme_metadeco import create_decorator

load = create_decorator("load", attributes={"stage": "load"})
compute = create_decorator("compute", attributes={"stage": "compute"})
save = create_decorator("save", attributes={"stage": "save"})
