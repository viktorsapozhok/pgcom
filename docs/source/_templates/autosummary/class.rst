{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :exclude-members:

   {% block methods %}
   {% if methods %}
   .. rubric:: Methods

   .. autosummary::
      :toctree:

   {% for item in methods %}
   {%- if not item.startswith('_') or item in ['__call__'] %}   ~{{ name }}.{{ item }}
   {% endif %}
   {%- endfor %}
   {% endif %}
   {% endblock %}
   {% block attributes %}
   {% if attributes %}
   .. rubric:: Properties

   .. autosummary::
      :toctree:

   {% for item in attributes %}
   {%- if not item.startswith('_') or item in ['__call__'] %}
   {%- if fullname in ['elbas_des.des.order.Order'] %}
   {%- if item not in ['orderid', 'order_dir', 'createdat', 'updatedat', 'price', 'qty', 'owner', 'deleted'] %}   ~{{ name }}.{{ item }}
   {% endif %}
   {%- elif fullname in ['elbas_des.des.event_queue.Event'] %}
   {%- if item not in ['time', 'kind', 'priority', 'tie_breaker', 'name', 'data'] %}   ~{{ name }}.{{ item }}
   {% endif %}
   {%- else %}   ~{{ name }}.{{ item }}
   {% endif %}
   {% endif %}
   {%- endfor %}
   {% endif %}
   {% endblock %}
