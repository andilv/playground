import { NostrEvent } from '@nostrify/nostrify';
import { nip19 } from 'nostr-tools';
import { useAuthor } from '@/hooks/useAuthor';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function PostCard({ post }: { post: NostrEvent }) {
  const author = useAuthor(post.pubkey);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-4">
          <Avatar>
            <AvatarImage src={author?.picture} />
            <AvatarFallback>{author?.name?.slice(0, 2) || '??'}</AvatarFallback>
          </Avatar>
          <div>
            <CardTitle>{author?.name || nip19.npubEncode(post.pubkey).slice(0, 12)}</CardTitle>
            <p className="text-sm text-gray-500">
              {new Date(post.created_at * 1000).toLocaleString()}
            </p>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <p>{post.content}</p>
      </CardContent>
    </Card>
  );
}
import { NostrEvent } from '@nostrify/nostrify';
import { nip19 } from 'nostr-tools';
import { useAuthor } from '@/hooks/useAuthor';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function PostCard({ post }: { post: NostrEvent }) {
  const author = useAuthor(post.pubkey);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-4">
          <Avatar>
            <AvatarImage src={author?.picture} />
            <AvatarFallback>{author?.name?.slice(0, 2) || '??'}</AvatarFallback>
          </Avatar>
          <div>
            <CardTitle>{author?.name || nip19.npubEncode(post.pubkey).slice(0, 12)}</CardTitle>
            <p className="text-sm text-gray-500">
              {new Date(post.created_at * 1000).toLocaleString()}
            </p>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <p>{post.content}</p>
      </CardContent>
    </Card>
  );
}


