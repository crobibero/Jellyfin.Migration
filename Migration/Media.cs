using System.Collections.Generic;

namespace Migration
{
    public class Media
    {
        public string Id { get; set; }
        public Provider ProviderIds { get; set; }
        
        public string Name { get; set; }
        public string SeriesName { get; set; }
        
        public int? IndexNumber { get; set; }
        public int? ParentIndexNumber { get; set; }
    }

    public class Provider
    {
        public string imdb { get; set; }
        public string tvdb { get; set; }
    }


    public class MediaContainer
    {
        public List<Media> Items { get; set; }
    }
}