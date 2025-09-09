import { useSeoMeta } from '@unhead/react';
import { Feed } from '@/components/Feed';

// FIXME: Update this page (the content is just a fallback if you fail to update the page)

const Index = () => {
  useSeoMeta({
    title: 'Nostr Feed',
    description: 'A simple Nostr feed viewer built with MKStack.',
  });

  return (
    <div className="flex justify-center py-8">
      <div className="w-full max-w-2xl">
        <h1 className="text-3xl font-bold mb-6 text-center">Nostr Feed</h1>
        <Feed />
      </div>
    </div>
  );
};

export default Index;

