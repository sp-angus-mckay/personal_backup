-WHERE I GOT TO - created AUs table, need to create revenues table and do further analysis on AUs and revenues uplift.

-Careful with the day counter for the benchmark averaging when not working in calendar days - maybe knock a bit off the start and end day counter as need be to account for proportionate days

-For active users analysis, note that total of the active users per hour will be higher than total of active users per day etc as one active user can be counted in several hour period as a unique user each hour, but will only be counted once over a 24 hour period in the daily analysis

-Active users uplift is not exact due to way the data is pulled from redshift (it takes exact hour/day periods and then splits these proportionally over the analysis hour/day periods)

-To compare active users to NGUs… i.e. want to separate uplift in active users due to NGUs to uplift due to reawakened users - can i directly take out the NGUs?

-Also think, does benchmark work ok with the way active users is done??

-Need to work out best way to represent revenues uplift. Simple to show standard uplift over expected, but also interesting to look at the revenues that the NGUs bring. Possibly need to concentrate on individual country here as RPI is different for different country etc.

-Could add an ‘if 0 then 1’ or ‘if 0 then take prev. number’ or something to the benchmark calculation to stop it breaking if it happens to be 0 on the campaign start hour/day/q_day

-Not sure if the way of rolling forward benchmark by same % change in benchmark is the best way as it can lead to quite different outcomes when looking at different time periods
