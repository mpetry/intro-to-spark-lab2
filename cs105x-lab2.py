# 4 a
# DataFrame containing all accesses that did not return a code 200
from pyspark.sql.functions import desc
not200DF = logs_df.where(col("status") != 200)
not200DF.show(10)
# Sorted DataFrame containing all paths and the number of times they were accessed with non-200 return code
logs_sum_df = not200DF.groupBy("path").count().sort("count", ascending=False)

print 'Top Ten failed URLs:'
logs_sum_df.show(10, False)

# 4b

# Number of unique hosts

unique_host_count = logs_df.select(col('host')).distinct().count()

print 'Unique hosts: {0}'.format(unique_host_count)

# 4c

# Unique daily hosts

from pyspark.sql.functions import dayofmonth

day_to_host_pair_df = logs_df.select(logs_df.host, dayofmonth(logs_df.time).alias('day')).cache()
day_group_hosts_df = day_to_host_pair_df.distinct()
daily_hosts_df = day_group_hosts_df.groupBy('day').count().sort('day', ascending = True).cache()

print 'Unique hosts per day:'
daily_hosts_df.show(30, False)


# 4d

# Prepare arrays for plotting

days_with_hosts = daily_hosts_df.map(lambda r: (r[0])).take(30)
hosts = daily_hosts_df.map(lambda r: (r[1])).take(30)
# for <FILL IN>:
#  <FILL IN>

print(days_with_hosts)
print(hosts)

# Plot daily unique hosts by day

fig, ax = prepareSubplot(np.arange(0, 30, 5), np.arange(0, 5000, 1000))
colorMap = 'Dark2'
cmap = cm.get_cmap(colorMap)
plt.plot(days_with_hosts, hosts, color=cmap(0), linewidth=3)
plt.axis([0, max(days_with_hosts), 0, max(hosts)+500])
plt.xlabel('Day')
plt.ylabel('Hosts')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

# Databricks plotting

display(daily_hosts_df)

# 4e

# Determine average requests per host per day (total daily requests / unique daily requests)

total_reqs_per_day_df = day_to_host_pair_df.groupBy('day').count()

avg_daily_req_per_host_df = (
  total_reqs_per_day_df
  .join(daily_hosts_df, total_reqs_per_day_df.day == daily_hosts_df.day)
  .select(daily_hosts_df['day'], 
          (total_reqs_per_day_df['count']/daily_hosts_df['count'])
          .alias("avg_reqs_per_host_per_day")).cache())


print 'Average number of daily requests per Hosts is:\n'
avg_daily_req_per_host_df.show()

# 4f

# Prepare arrays for days and average daily requests for plotting

days_with_avg = (avg_daily_req_per_host_df.map(lambda r: (r[0])).take(30))
avgs = (avg_daily_req_per_host_df.map(lambda r: (r[1])).take(30))
# for <FILL IN>:
#   <FILL IN>

print(days_with_avg)
print(avgs)

# Plot

fig, ax = prepareSubplot(np.arange(0, 20, 5), np.arange(0, 16, 2))
colorMap = 'Set3'
cmap = cm.get_cmap(colorMap)
plt.plot(days_with_avg, avgs, color=cmap(0), linewidth=3)
plt.axis([0, max(days_with_avg), 0, max(avgs)+2])
plt.xlabel('Day')
plt.ylabel('Average')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

# Databricks plotting

display(avg_daily_req_per_host_df)

# 5a

# Looking at 404 status codes

not_found_df = logs_df.where(logs_df.status == 404).cache()
print('Found {0} 404 URLs').format(not_found_df.count())

# 5b 

# Generate dataframe of unique 404 paths

not_found_paths_df = not_found_df.select(not_found_df.path)
unique_not_found_paths_df = not_found_paths_df.distinct()

print '404 URLS:\n'
unique_not_found_paths_df.show(n=40, truncate=False)

# 5c Find top most common 404 URLs

top_20_not_found_df = not_found_paths_df.groupBy('path').count().sort('count', ascending=False).cache()

print 'Top Twenty 404 URLs:\n'
top_20_not_found_df.show(n=20, truncate=False)

# 5d

# Find top most common 404 hosts

hosts_404_count_df = not_found_df.groupBy('host').count().sort('count', ascending=False).cache()

print 'Top 25 hosts that generated errors:\n'
hosts_404_count_df.show(n=25, truncate=False)

# 5e

# Listing 404 errors by date

errors_by_date_sorted_df = not_found_df.groupBy(dayofmonth('time').alias('day')).count().sort('day', ascending=True).cache()

print '404 Errors by day:\n'
errors_by_date_sorted_df.show()

# 5f

# Visualizing 404 errors by day. Prepare arrays

days_with_errors_404 = (errors_by_date_sorted_df.map(lambda r: (r[0])).take(30))
errors_404_by_day = (errors_by_date_sorted_df.map(lambda r: (r[1])).take(30))
#for <FILL IN>:
#  <FILL IN>

print days_with_errors_404
print errors_404_by_day

# Plot 404 errors by day

fig, ax = prepareSubplot(np.arange(0, 20, 5), np.arange(0, 600, 100))
colorMap = 'rainbow'
cmap = cm.get_cmap(colorMap)
plt.plot(days_with_errors_404, errors_404_by_day, color=cmap(0), linewidth=3)
plt.axis([0, max(days_with_errors_404), 0, max(errors_404_by_day)])
plt.xlabel('Day')
plt.ylabel('404 Errors')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

# Use databricks plot

display(errors_by_date_sorted_df)

# 5g

# Top days for 404 errors

top_err_date_df = errors_by_date_sorted_df.sort('count', ascending=False)

print 'Top Five Dates for 404 Requests:\n'
top_err_date_df.show(5)

# 5h

# Sort 

from pyspark.sql.functions import hour

hour_records_sorted_df = not_found_df.groupBy(hour('time').alias('hour')).count().sort('hour', ascending=True).cache()

print 'Top hours for 404 requests:\n'
hour_records_sorted_df.show(24)

# 5i

# Plot 404 errors by hour

hours_with_not_found = [(row[0]) for row in hour_records_sorted_df.collect()]
not_found_counts_per_hour = [(row[1]) for row in hour_records_sorted_df.collect()]

print hours_with_not_found
print not_found_counts_per_hour

fig, ax = prepareSubplot(np.arange(0, 25, 5), np.arange(0, 500, 50))
colorMap = 'seismic'
cmap = cm.get_cmap(colorMap)
plt.plot(hours_with_not_found, not_found_counts_per_hour, color=cmap(0), linewidth=3)
plt.axis([0, max(hours_with_not_found), 0, max(not_found_counts_per_hour)])
plt.xlabel('Hour')
plt.ylabel('404 Errors')
plt.axhline(linewidth=3, color='#999999')
plt.axvline(linewidth=2, color='#999999')
display(fig)

# Databricks plotting

display(hour_records_sorted_df)