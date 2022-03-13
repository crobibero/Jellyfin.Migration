namespace Jellyfin.Migration.Models;

public class Media
{
    public Guid Id { get; init; }
    public Provider ProviderIds { get; init; }
        
    public string Name { get; init; }
    public string SeriesName { get; init; }
        
    public int? IndexNumber { get; init; }
    public int? ParentIndexNumber { get; init; }
}

public class Provider
{
    public string Imdb { get; init; }
    public string Tvdb { get; init; }
}


public class MediaContainer
{
    public List<Media> Items { get; init; }
}