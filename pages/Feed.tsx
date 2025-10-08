import { useInView } from 'react-intersection-observer';
import { useMemo, useEffect } from 'react';
import { useGlobalFeed } from '@/hooks/useGlobalFeed';
import { PostCard } from '@/components/PostCard';
import { Skeleton } from '@/components/ui/skeleton';

export function Feed() {
  const { data, fetchNextPage, hasNextPage, isFetchingNextPage } = useGlobalFeed();
  const { ref, inView } = useInView();

  useEffect(() => {
    if (inView && hasNextPage && !isFetchingNextPage) {
      fetchNextPage();
    }
  }, [inView, hasNextPage, isFetchingNextPage, fetchNextPage]);

  // Remove duplicate events by ID
  const posts = useMemo(() => {
    const seen = new Set();
    return data?.pages.flat().filter(event => {
      if (!event.id || seen.has(event.id)) return false;
      seen.add(event.id);
      return true;
    }) || [];
  }, [data?.pages]);

  return (
    <div className="space-y-4">
      {posts.map((post) => (
        <PostCard key={post.id} post={post} />
      ))}
      {hasNextPage && (
        <div ref={ref} className="py-4">
          {isFetchingNextPage && <Skeleton className="h-20 w-full" />}
        </div>
      )}
    </div>
  );
}
