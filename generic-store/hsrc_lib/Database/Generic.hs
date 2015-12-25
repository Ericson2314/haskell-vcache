{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Generic where

import qualified Data.Bytes.Get as B
import qualified Data.Bytes.Put as B
import           Data.Typeable


class (MonadGet (Get d) (Ref d), MonadPut (Put d) (Ref d)) => Store d where
  -- | Immutable Reference
  type Ref d :: * -> *

  -- | Virtual Address Space distinguisher
  type Space d

  type Put d :: * -> *
  type Get d :: * -> *

  ref   :: Persistable a => Space d -> a -> Ref d a
  deref :: Persistable a => Ref d a -> a


class ( MonadGetMut (Get d) (Ref d) (Var d)
      , MonadPutMut (Put d) (Ref d) (Var d)
      , Store d)
      => StoreMut d where

  -- | Mutable Variable
  type Var d :: * -> *


class B.MonadPut m => MonadPut m ref | m -> ref where
  -- | Store a reference to a value. The value reference must use the
  -- same database instances and address space as where you're putting it.
  putRef :: forall a. Persistable a => ref a -> m ()

class B.MonadGet m => MonadGet m ref | m -> ref where
  -- | Load a Ref, just the reference rather than the content. User must know
  -- the type of the value, since getRef is essentially a typecast.
  getRef :: forall a. Persistable a => m (ref a)

class MonadPut m ref => MonadPutMut m ref var | m -> ref var where
  -- | Store an identifier for a persistent variable in the same Database and
  -- address space.
  putVar :: forall a. PersistableMut a => var a -> m ()

class MonadGet m ref => MonadGetMut m ref var | m -> ref var where
  -- | Load a Var, just the variable. Developers must know the type of the Var,
  -- since getVar will cast to any cacheable type.
  getVar :: forall a. PersistableMut a => m (var a)


class (Typeable a) => Persistable a where
  -- | Serialize a value as a stream of bytes and references.
  put :: forall m r. MonadPut m r => a -> m ()

  -- | Parse a value from its serialized representation into memory.
  get :: forall m r. MonadGet m r => m a

class (Persistable a) => PersistableMut a where
  -- | Serialize a value as a stream of bytes, references, and variables.
  putMut :: forall m r v. MonadPutMut m r v => a -> m ()

  -- | Parse a value from its serialized representation into memory.
  getMut :: forall m r v. MonadGetMut m r v  => m a

instance (Persistable a) => PersistableMut a where
  putMut = put
  getMut = get
