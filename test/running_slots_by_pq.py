import pandas as pd
from datetime import datetime
from json import loads

from requests import post

headers = {}

headers[
    'Authorization'] = 'Bearer eyJrIjoiMkJBNkYzUzFMTW9ZcmxXOXVlU3BVeGNmaWYwYnM5UEgiLCJuIjoiNE1hcmlhRyIsImlkIjoxN30='
headers['Content-Type'] = 'application/json'
headers['Accept'] = 'application/json'

base = "https://monit-grafana.cern.ch"
url = "api/datasources/proxy/9543/_msearch?max_concurrent_shard_requests=256"

query = """"{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_running*"]}
{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":1577836800000,"lte":1609804799000,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"((NOT _exists_:data.container_name) OR (data.container_name:*)) AND data.dst_experiment_site:* AND data.dst_cloud:* AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:* AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:* AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"3":{"terms":{"field":"data.adcactivity","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"1":{"sum":{"field":"data.wavg_actualcorecount","missing":0,"script":"_value / 1"}}}}}}
"""
query_orig = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_running*"]}
{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":1577836800000,"lte":1609804799000,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"((NOT _exists_:data.container_name) OR (data.container_name:*)) AND data.dst_experiment_site:* AND data.dst_cloud:* AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:* AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:* AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"3":{"terms":{"field":"data.adcactivity","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"1":{"sum":{"field":"data.wavg_actualcorecount","missing":0,"script":"_value / 1"}}}}}}
"""
request_url = "%s/%s" % (base, url)

r = post(request_url, headers=headers, data=query, timeout=99999)

result = []

if r.ok:
    sites = loads(r.text)['responses'][0]['aggregations']
    print(sites)
