import { useState } from 'react';
import { Button } from '../../ui/button';
import { Upload, Bookmark } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Topic } from '../types';

interface TopicConfigModalProps {
  topic: Topic | null; 
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function TopicConfigModal({ topic, open, onOpenChange }: TopicConfigModalProps) {
  const [messageType, setMessageType] = useState<string>('JSON');
  const [schemaRegistry, setSchemaRegistry] = useState<string>('');
  const [password, setPassword] = useState<string>('');
  const [username, setUsername] = useState<string>('');
  const [keystorePath, setKeystorePath] = useState<string>('');
  const [keystorePassword, setKeystorePassword] = useState<string>('');
  const [truststorePath, setTruststorePath] = useState<string>('');
  const [truststorePassword, setTruststorePassword] = useState<string>('');
  const [saslMechanism, setSaslMechanism] = useState<string>('PLAIN');

  const handleLoadFiles = () => {
    const input = document.createElement('input');
    input.type = 'file';
    input.multiple = true;
    input.accept = '.proto';
    input.onchange = (e) => {
      const files = (e.target as HTMLInputElement).files;
      if (files && files.length > 0) {
        toast.success(`Loaded ${files.length} Proto file(s)`);
      }
    };
    input.click();
  };

  const handleSave = () => {
    toast.success(`Configuration saved for topic: ${topic?.name}`);
    onOpenChange(false);
  };

  const handleTestConnection = () => {
    toast.success('Connection test successful');
  };

  const handleAddToFavorites = () => {
    toast.success(`Added "${topic?.name}" to favorites`);
  };

  const isSSLRequired = messageType === 'Avro';
  const isSASLRequired = messageType === 'Avro';

  if (!topic) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose 
        className="max-w-md bg-[#0f172b] border-[#314158] text-slate-50"
        aria-describedby={undefined}
      >
        <DialogHeader className="border-b border-[#314158] pb-4">
          <div className="flex items-center justify-between">
            <DialogTitle className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-lg">
              {topic.name}
            </DialogTitle>
            <button
              onClick={handleAddToFavorites}
              className="p-1 text-[#62748E] hover:text-[#ffb86a] transition-colors duration-200 cursor-pointer border-none bg-transparent outline-none"
              title="Add to favorites"
            >
              <Bookmark className="size-4" />
            </button>
          </div>
        </DialogHeader>
        
        <div className="space-y-6 pt-4">
          {/* Message Type */}
          <div className="space-y-2">
            <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
              Message Type
            </Label>
            <Select value={messageType} onValueChange={setMessageType}>
              <SelectTrigger className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-[#0f172b] border-[#314158]">
                <SelectItem value="JSON" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">JSON</SelectItem>
                <SelectItem value="Text" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">Text</SelectItem>
                <SelectItem value="Proto" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">Proto</SelectItem>
                <SelectItem value="Avro" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">Avro</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {/* Proto specific fields */}
          {messageType === 'Proto' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                  Proto Files
                </Label>
                <Button
                  onClick={handleLoadFiles}
                  variant="outline"
                  className="w-full bg-transparent border-[#314158] text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50 font-['Fira_Code:Retina',_sans-serif]"
                >
                  <Upload className="size-4 mr-2" />
                  Load Proto Files
                </Button>
              </div>
            </div>
          )}

          {/* Avro specific fields */}
          {messageType === 'Avro' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                  Schema Registry URL
                </Label>
                <Input
                  value={schemaRegistry}
                  onChange={(e) => setSchemaRegistry(e.target.value)}
                  placeholder="http://localhost:8081"
                  className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                />
              </div>
              <div className="space-y-2">
                <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                  Password
                </Label>
                <Input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Enter password"
                  className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                />
              </div>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-3 pt-4">
            <Button
              onClick={() => onOpenChange(false)}
              variant="outline"
              className="flex-1 bg-transparent border-[#314158] text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50 font-['Fira_Code:Retina',_sans-serif]"
            >
              Cancel
            </Button>
            <Button
              onClick={handleSave}
              className="flex-1 bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif]"
            >
              Save
            </Button>
          </div>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}