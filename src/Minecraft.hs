{-# LANGUAGE OverloadedStrings #-}

module Minecraft (
    getSourceFiles, traverseChunks, getChunkRefs, extractBlocks,
    openDatabase, filterNewerOnly,
    nbtGet, nbtKeys,
    chunkRefGetXZ,
    getChunkBlockData, 
    fromNBTInteger, fromNBTList, fromNBTString,
    NBT(..), ChunkRef(..), Coord(..), distance
)
where

import Data.Conduit

import System.FilePath (replaceExtension, takeFileName, takeBaseName, combine, takeExtension)
import System.Directory (getDirectoryContents)
import Control.Applicative ((<$>), (<|>), pure, (*>))
import Control.Monad (replicateM, when)
import Data.List (isPrefixOf)
import Control.Arrow (second)
import Data.List.Split (chunksOf)
import Data.Binary.IEEE754 (getFloat32be, getFloat64be)
import Data.Binary.Get (runGet)
import Data.Char (chr)
import Data.Int (Int64)
import System.IO (openBinaryFile, stderr, hPutStr, hClose, hSeek, IOMode(..), SeekMode(..), Handle)
import Codec.Compression.Zlib (decompress)
import Control.Monad.IO.Class (liftIO)
import Data.Bits (shift, (.|.), (.&.))
import Control.Monad.State.Lazy (StateT, lift, get, put, runStateT)
import Data.Maybe (catMaybes, fromJust, listToMaybe)
import Database.SQLite.Simple (NamedParam((:=)))
import Data.Word (Word8)

import qualified Database.SQLite.Simple as SQL
import qualified Data.Map.Lazy as Map
import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL



getSourceFiles :: String -> Source IO (Int,FilePath)
getSourceFiles root = 
    do dimNames <- lift $ filter (isPrefixOf "DIM") <$> getDirectoryContents root
       let dirs = map (second makePath) $ (0,"") : map ( (,) =<< read . drop 3) dimNames
         in do
            files <- concat <$> mapM getFiles dirs
            CL.sourceList files
  where gci f = do  l <- lift $ getDirectoryContents f
                    let l' = filter (\ ff -> ff /= "." && ff /= "..") l
                      in return $ map (combine f) l'
        makePath d = combine root $ combine d "region"
        getFiles (n, d) = map ((,) n) <$> filter ((==".mca").takeExtension) <$> gci d

openDatabase :: String -> IO SQL.Connection
openDatabase n = 
    let dbfn = replaceExtension (takeFileName n) ".db"
      in do h <- SQL.open dbfn
            SQL.execute_ h "PRAGMA journal_mode=WAL"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS chunks (file, x, z, cx, cz, ts)"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS villagers (uuid, cid, profession, x, y, z, dim)"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS trades (vid, buy1, cnt1, buy2, cnt2, sell, scnt, ench, remaining)"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS frontier (x, z)"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS chests (cid, x, y, z, dim)"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS chestcontents (chid, cnt, slot, id, damage, tag)"
            SQL.execute_ h "CREATE TABLE IF NOT EXISTS portals (cid, x, y, z, dim)"

            SQL.execute_ h "CREATE INDEX IF NOT EXISTS chunksIx on chunks (file, dim, x, z)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS villagersIx on villagers (uuid)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS villagersIxCid on villagers (cid)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS tradesIx on trades (vid)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS frontierIx on frontier (x, z)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS chestsIx on chests (dim,x,y,z)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS chestsIxCid on chests (cid)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS chestcontentsIx on chestcontents (chid)"
            SQL.execute_ h "CREATE INDEX IF NOT EXISTS portalsIx on portals (cid)"
            
            return h
            
traverseChunks :: Conduit ChunkRef IO (ChunkRef,NBT) 
traverseChunks = tc' Nothing
    where tc' st = do ma <- await
                      case ma of 
                          Nothing -> return ()
                          Just c -> do 
                              (d,st') <- lift $ runStateT (readChunk c) st
                              yield (c,d)
                              tc' st'
            
chunkRefGetXZ :: ChunkRef -> (Int, Int)
chunkRefGetXZ (ChunkRef f _ cx cz _ _ _ _) = 
    let bn = takeBaseName f
        (_, _ : t) = break (=='.') bn
        (xStr, _ : zStr) = break (=='.') t
        rx = read xStr :: Int
        rz = read zStr :: Int
      in (16*((rx*32)+cx), 16*((rz*32)+cz))

data ChunkRef = ChunkRef { _f :: FilePath, _dim :: Int, _x :: Int, _z :: Int, _ofs :: Int, _siz :: Int, _ts :: Int, _chunkid :: Maybe Int64 }
  deriving (Eq, Show)
  
data Coord = Coord {
        coordDim :: Integer,
        coordX :: Integer,
        coordY :: Integer,
        coordZ :: Integer 
    } deriving (Eq, Ord)

instance Show Coord where
    show (Coord d x y z) = "(" ++ show x ++ "," ++ show y ++ "," ++ show z ++ ")@" ++ show d
    
    
distance :: Floating s => Coord -> Coord -> Maybe s
distance (Coord d1 x1 y1 z1) (Coord d2 x2 y2 z2) 
    | d1 == d2  = Just $ sqrt $ fromIntegral $ dx*dx+dy*dy+dz*dz
    | otherwise = Nothing   
  where dx = x1 - x2
        dy = y1 - y2
        dz = z1 - z2
          
extractBlocks :: Conduit (ChunkRef, NBT) IO (ChunkRef,Coord,Integer)
extractBlocks = awaitForever $
    \(cr,d) -> CL.sourceList $ map (uncurry ((,,) cr)) $ getChunkBlockData (_dim cr) d
  
getChunkBlockData :: Int -> NBT -> [(Coord, Integer)]      
getChunkBlockData dim d =
    let level = nbtGet "Level" $ Just d
        (Just cx) = fromNBTInteger $ nbtGet "xPos" level
        (Just cz) = fromNBTInteger $ nbtGet "zPos" level
        (Just (NBTList sections)) = nbtGet "Sections" level
     in concatMap (expandSection dim ((* 16) cx, (* 16) cz)) sections
           
expandSection :: Int -> (Integer,Integer) -> NBT -> [(Coord,Integer)]
expandSection dim (sx,sz) section =
    let sy = (*16) $ fromJust (fromNBTInteger $ nbtGet "Y" (Just section) <|> error "No Y in section")
        coords = [Coord (fromIntegral dim) (x+sx) (y+sy) (z+sz) | y <- [0..15], z <- [0..15], x <- [0..15] ]
        Just (NBTByteArray blocks') = nbtGet "Blocks" $ Just section
        add = case nbtGet "Add" $ Just section of 
            (Just (NBTByteArray add''')) -> 
                    concatMap (\a -> [a .&. 0x0F, (a `shift` (-4)) .&. 0x0f]) add'''
            _ -> repeat 0
        blocks = zipWith (\b a -> b .|. (a `shift` 8)) blocks' add
      in zip coords blocks
  
fromNBTString :: Maybe NBT -> Maybe String
fromNBTString (Just (NBTString i)) = Just i
fromNBTString _ = Nothing  
  
fromNBTInteger :: Maybe NBT -> Maybe Integer
fromNBTInteger (Just (NBTLong i)) = Just i  
fromNBTInteger (Just (NBTInt i)) = Just i  
fromNBTInteger (Just (NBTShort i)) = Just i  
fromNBTInteger (Just (NBTByte i)) = Just i  
fromNBTInteger _ = Nothing

fromNBTList :: Maybe NBT -> [NBT]
fromNBTList (Just (NBTList l)) = l
fromNBTList _ = []

nbtGet :: String -> Maybe NBT -> Maybe NBT
nbtGet t (Just (NBTCompound m)) = Map.lookup t m
nbtGet _ _ = Nothing

nbtKeys :: Maybe NBT -> Maybe [String]
nbtKeys (Just (NBTCompound m)) = Just (Map.keys m)
nbtKeys _ = Nothing
                                      
getChunkRefs :: Conduit (Int,FilePath) IO ChunkRef
getChunkRefs = awaitForever $ \f -> do 
    r <- liftIO $ processFile f
    CL.sourceList r

processFile :: (Int,FilePath) -> IO [ChunkRef]
processFile (dim,f) = 
    do h <- openBinaryFile f ReadMode
       rawLoc <- B.hGet h 4096
       rawTms <- B.hGet h 4096
       let xz = [(x,z) | z<-[0..31], x<-[0..31]]
           p _ Nothing _  = Nothing
           p (x,z) (Just (o,s)) t = Just (ChunkRef f dim x z o s t Nothing)
           loc = unpackRaw unpackLoc rawLoc
           tms = unpackRaw unpackTms rawTms
           chunkrefs = catMaybes $ zipWith3 p xz loc tms
         in return chunkrefs

unpackLoc :: (Num t1, Num t2) => [Word8] -> Maybe (t1, t2)
unpackLoc b | b == [0,0,0,0] = Nothing
            | otherwise      = Just (fromIntegral $ getInt (take 3 b), fromIntegral (toInteger (b !! 3)))

unpackTms :: Num b => [Word8] -> b
unpackTms b = fromIntegral $ getInt b

getInt :: [Word8] -> Integer
getInt = foldl (\ b a -> toInteger a .|. (b `shift` 8)) 0

toSigned32 :: (Ord a, Num a) => a -> a
toSigned32 l = if l >= 0x80000000 then l - 0x100000000 else l
  
unpackRaw ::  ([Word8] -> t) -> B.ByteString -> [t]
unpackRaw f r | B.null r  = []
              | otherwise = f (B.unpack $ B.take 4 r) : unpackRaw f (B.drop 4 r)
                  
type ReadChunkSt = Maybe (FilePath, Handle)

filterNewerOnly :: SQL.Connection -> Conduit ChunkRef IO ChunkRef
filterNewerOnly db = awaitForever $ \cr ->
    let (ChunkRef fn dim x z o s t _) = cr
        (cx,cz) = chunkRefGetXZ cr
    in do r <- lift $ listToMaybe <$> SQL.queryNamed db
                "Select rowid, ts, cx is null or cz is null from chunks where file = :file and x = :x and z = :z and dim = :dim" 
                [":file" := fn, ":x" := x, ":z" := z, ":dim" := dim]
          (cid,use) <- case r of 
                Nothing -> do   lift $ SQL.executeNamed db
                                    "INSERT INTO chunks (file, x, z, cx, cz, dim, ts) VALUES (:file, :x, :z, :cx, :cz, :dim, 0)" 
                                    [":file" := fn, ":x" := x, ":z" := z, ":dim" := dim, ":cx" := cx, ":cz" := cz]
                                newcid <- lift $ SQL.lastInsertRowId db
                                return (newcid, True)
                (Just (cid,t',_)) -> 
                               return (cid, t > t')
          case r of 
                (Just (_,_,True)) -> 
                    lift $ SQL.executeNamed db
                                "UPDATE chunks set cx = :cx, cz = :cz WHERE rowid = :rowid"
                                [":cx" := cx, ":cz" := cz, ":rowid" := cid]
                _ -> return ()
          when use $
              do yield (ChunkRef fn dim x z o s t (Just cid))
                 lift $
                   SQL.executeNamed db
                     "UPDATE chunks SET ts = :ts WHERE rowid = :rowid"
                     [":ts" := t, ":rowid" := cid]
                
            


readChunk :: ChunkRef -> StateT ReadChunkSt IO NBT
readChunk (ChunkRef f _ _ _ o s _ _) =
    do st <- get
       h <- liftIO $ openFile st
       put $ Just (f,h)
       liftIO $ hSeek h AbsoluteSeek (toInteger (o * 4096))
       rawData <- liftIO $ B.hGet h (s * 4096)
       let _ = getInt $ B.unpack $ B.take 4 rawData -- data length
           _ = B.index rawData 4 -- compression type, not checked FIXME
           zData = B.drop 5 rawData
           d = BL.toStrict $ decompress $ BL.fromStrict zData
           Right ("",chunkContent) = parseNBT d
         in return chunkContent
  where openFile (Just (f',h')) | f' == f = return h'
                                | otherwise = do hClose h'; open'
        openFile Nothing = open'
        open' = do hPutStr stderr $ "Reading chunks from " ++ f ++ "\n"
                   openBinaryFile f ReadMode

data NBT = NBTEmpty |
           NBTByte Integer |
           NBTShort Integer |
           NBTInt Integer |
           NBTLong Integer |
           NBTFloat Float |
           NBTDouble Double |
           NBTByteArray [Integer] |
           NBTString String |
           NBTIntArray [Integer] |
           NBTList [NBT] |          -- NBTLists are actually homogenous
           NBTCompound (Map.Map String NBT)
  deriving (Eq, Show)

parseNBT :: B.ByteString -> Either String (String, NBT)
parseNBT = A.parseOnly nbtParser

nbtParser :: A.Parser (String, NBT)
nbtParser = A.word8 0 *> pure ("",NBTEmpty) <|>
            do t <- A.notWord8 0; n <- readString; v <- parseSingle t; return (n,v)

readByte :: A.Parser Integer
readByte   = do b <- A.take 1; return $ getInt $ B.unpack b

readShort :: A.Parser Integer
readShort  = do b <- A.take 2; return $ getInt $ B.unpack b

readInt :: A.Parser Integer
readInt    = do b <- A.take 4; return $ toSigned32 $ getInt $ B.unpack b

readString :: A.Parser String
readString = do n <- readShort
                s <- A.take (fromIntegral n)
                return $ map (chr . fromIntegral) $ B.unpack s

parseNBTByte :: A.Parser NBT
parseNBTByte   = NBTByte  <$> readByte
parseNBTShort :: A.Parser NBT
parseNBTShort  = NBTShort <$> readShort
parseNBTInt :: A.Parser  NBT
parseNBTInt    = NBTInt   <$> readInt

parseNBTLong :: A.Parser NBT
parseNBTLong   = do b <- A.take 8; return $ NBTLong   $ getInt $ B.unpack b
parseNBTFloat :: A.Parser NBT
parseNBTFloat  = do b <- A.take 4; return $ NBTFloat  $ runGet getFloat32be $ BL.fromStrict b
parseNBTDouble :: A.Parser NBT
parseNBTDouble = do b <- A.take 8; return $ NBTDouble $ runGet getFloat64be $ BL.fromStrict b
parseNBTByteArray :: A.Parser NBT
parseNBTByteArray = do n <- readInt
                       bytes <- B.copy <$> A.take (fromIntegral n)
                       return $ NBTByteArray $ map fromIntegral $ B.unpack bytes
parseNBTIntArray :: A.Parser NBT
parseNBTIntArray  = do n <- readInt
                       bytes <- B.copy <$> A.take (fromIntegral (n * 4))
                       return $ NBTIntArray $ map (fromIntegral . getInt) $ chunksOf 4 $ B.unpack bytes
parseNBTString :: A.Parser NBT
parseNBTString = NBTString <$> readString
parseSingle :: Word8 -> A.Parser NBT
parseNBTList :: A.Parser NBT
parseNBTList = do t <- A.anyWord8
                  n <- readInt
                  NBTList <$> replicateM (fromIntegral n) (parseSingle t)

parseNBTCompound :: A.Parser NBT
parseNBTCompound = do x <- f' Map.empty; return $ NBTCompound x
  where f' l = do v <- nbtParser
                  case snd v of
                    NBTEmpty -> return l
                    _        -> f' (uncurry Map.insert v l)

parseSingle 0 = return NBTEmpty
parseSingle 1 = parseNBTByte
parseSingle 2 = parseNBTShort
parseSingle 3 = parseNBTInt
parseSingle 4 = parseNBTLong
parseSingle 5 = parseNBTFloat
parseSingle 6 = parseNBTDouble
parseSingle 7 = parseNBTByteArray
parseSingle 8 = parseNBTString
parseSingle 9 = parseNBTList
parseSingle 10 = parseNBTCompound
parseSingle 11 = parseNBTIntArray
parseSingle n  = error $ "Invalid NBT tag type: " ++ show n

            