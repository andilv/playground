import { Event } from "nostr-tools";
import { useAuthor } from "@nostrify/react";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { NoteContent } from "./NoteContent"; // Assuming this component exists or will be created

interface NoteProps {
  event: Event;
}

export function Note({ event }: NoteProps) {
  const { author } = useAuthor({ pubkey: event.pubkey });

  return (
    <Card>
      <CardHeader className="flex flex-row items-center gap-4">
        <Avatar>
          <AvatarImage src={author?.picture} />
          <AvatarFallback>{author?.name?.charAt(0) || event.pubkey.slice(0, 2)}</AvatarFallback>
        </Avatar>
        <div className="flex flex-col">
          <span className="font-bold">{author?.name || event.pubkey.slice(0, 8)}</span>
          <span className="text-sm text-gray-500">{new Date(event.created_at * 1000).toLocaleString()}</span>
        </div>
      </CardHeader>
      <CardContent>
        <NoteContent content={event.content} />
      </CardContent>
    </Card>
  );
}

