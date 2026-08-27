import { Button } from '../../ui/button';
import { Star, Trash2 } from 'lucide-react';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { FavoriteMessage, FullMessage } from '../types';

interface FavoritesModalProps {
  favorites: FavoriteMessage[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRemoveFavorite: (message: FavoriteMessage) => void;
  onSelectMessage: (message: FullMessage) => void;
}

export function FavoritesModal({ favorites, open, onOpenChange, onRemoveFavorite, onSelectMessage }: FavoritesModalProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-5xl max-h-[90vh] bg-surface border-edge text-slate-50">
        <DialogHeader className="border-b border-edge pb-4">
          <DialogTitle className="font-mono text-soft text-lg">
            Favorite Messages ({favorites.length})
          </DialogTitle>
          <DialogDescription className="font-mono text-dim text-sm">
            Add messages to your favorites by clicking the star icon in message details
          </DialogDescription>
        </DialogHeader>
        
        <div className="flex-1 overflow-auto">
          {favorites.length === 0 ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <Star className="size-12 text-dim mx-auto mb-4" />
                <div className="font-mono text-soft text-lg mb-2">
                  No favorite messages yet
                </div>
                <div className="font-mono text-dim text-sm">
                  Add messages to your favorites by clicking the star icon in message details
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-4 pt-4">
              {favorites.map((favorite, index) => (
                <div 
                  key={`${favorite.topicName}-${favorite.partition}-${favorite.offset}-${index}`} 
                  className="border border-edge rounded-lg p-4 hover:bg-elevated transition-colors cursor-pointer"
                  onClick={() => onSelectMessage(favorite)}
                >
                  {/* First row: Topic name (left) + Delete button (right) */}
                  <div className="flex items-center justify-between mb-2">
                    <div className="font-mono text-brand">
                      {favorite.topicName}
                    </div>
                    <Button
                      onClick={(e: any) => {
                        e.stopPropagation();
                        onRemoveFavorite(favorite);
                      }}
                      variant="outline"
                      size="sm"
                      className="bg-transparent border-edge text-danger hover:bg-edge hover:text-danger-hover"
                    >
                      <Trash2 className="size-4" />
                    </Button>
                  </div>
                  
                  {/* Second row: partition and offset */}
                  <div className="flex items-center gap-4 mb-2">
                    <div className="font-mono text-soft text-sm">
                      partition: {favorite.partition}
                    </div>
                    <div className="font-mono text-soft text-sm">
                      offset: {favorite.offset}
                    </div>
                  </div>
                  
                  {/* Third row: Saved timestamp */}
                  <div className="font-mono text-dim text-sm mb-3">
                    Saved: {new Date(favorite.savedAt).toLocaleString()}
                  </div>
                  
                  {/* Fourth row: Message content */}
                  <div className="bg-sunken border border-edge rounded-lg p-3 max-h-32 overflow-auto">
                    <div className="font-mono text-sm text-soft truncate">
                      {favorite.value}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}