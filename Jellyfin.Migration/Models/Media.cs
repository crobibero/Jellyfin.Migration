namespace Jellyfin.Migration.Models;

public class Media
{
    public string Id { get; init; }

    public string Path { get; init; }
    
    public Provider ProviderIds { get; init; }
        
    public string Name { get; init; }
    
    public UserData UserData { get; init; }
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

public class UserData
{
    public DateTime? LastPlayedDate { get; init; }
}
