SELECT c.ChannelName, 
       v.Title,  
       v.Published_date, 
       c.Views/c.NumVids as avgviews,  
       c.Subscribers, 
       c.Views as totalchannelviews, 
       v.views, 
       v.views/c.Subscribers as viewspersubscriber, 
       v.Likes+v.Comments as engagements,  
       (v.Likes+v.Comments)/c.Subscribers as engagementspersubscriber

FROM channels as c
join videos as v
on c.ChannelName = v.Channel;
