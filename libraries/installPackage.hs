
import Distribution.PackageDescription
import Distribution.Simple
import Distribution.Simple.Configure
import Distribution.Simple.LocalBuildInfo
import Distribution.Simple.Program
import Distribution.Simple.Setup
import Distribution.Simple.Utils
import Distribution.Verbosity
import System.Environment

main :: IO ()
main
  = do args <- getArgs
       case args of
           destdir : ipref : ibindir : ilibdir : ilibexecdir
                   : idatadir : idocdir : ihtmldir_copy : ihtmldir_reg
                   : ghcpkg : ghcpkgconf : args' ->
               let verbosity = case args' of
                           [] -> normal
                           ['-':'v':v] ->
                               let m = case v of
                                           "" -> Nothing
                                           _ -> Just v
                               in flagToVerbosity m
                           _ -> error ("Bad arguments: " ++ show args)
               in doit destdir ipref ibindir ilibdir
                       ilibexecdir idatadir idocdir
                       ihtmldir_copy ihtmldir_reg
                       ghcpkg ghcpkgconf verbosity
           _ ->
               error "Missing arguments"

doit :: FilePath -> FilePath -> FilePath -> FilePath -> FilePath
     -> FilePath -> FilePath -> FilePath -> FilePath -> FilePath
     -> FilePath
     -> Verbosity
     -> IO ()
doit destdir ipref ibindir ilibdir ilibexecdir idatadir idocdir
     ihtmldir_copy ihtmldir_reg ghcpkg ghcpkgconf verbosity =
       do let userHooks = simpleUserHooks
              copyto = if null destdir then NoCopyDest else CopyTo destdir
              copyFlags = (emptyCopyFlags copyto) {
                              copyVerbose = verbosity
                          }
              registerFlags = emptyRegisterFlags {
                                  regPackageDB = Just GlobalPackageDB,
                                  regVerbose = verbosity,
                                  regGenScript = False,
                                  regInPlace = False
                              }
          lbi <- getConfig verbosity
          let pd = localPkgDescr lbi
              i = installDirTemplates lbi
              -- XXX This is an almighty hack, shadowing the base
              -- Setup.hs hack
              mkLib filt = case library pd of
                           Just lib ->
                               let ems = filter filt $ exposedModules lib
                               in lib {
                                      exposedModules = ems
                                   }
                           Nothing ->
                               error "Expected a library, but none found"
              -- There's no files for GHC.Prim, so we will fail if we
              -- try to copy them
              pd_copy = pd { library = Just (mkLib ("GHC.Prim" /=)) }
              pd_reg  = pd { library = Just (mkLib (const True)) }
              -- When coying, we need to actually give a concrete
              -- directory to copy to rather than "$topdir"
              i_copy = i { prefixDirTemplate  = toPathTemplate ipref,
                           binDirTemplate     = toPathTemplate ibindir,
                           libDirTemplate     = toPathTemplate ilibdir,
                           libexecDirTemplate = toPathTemplate ilibexecdir,
                           dataDirTemplate    = toPathTemplate idatadir,
                           docDirTemplate     = toPathTemplate idocdir,
                           htmlDirTemplate    = toPathTemplate ihtmldir_copy
                         }
              lbi_copy = lbi { installDirTemplates = i_copy }
              -- When we run GHC we give it a $topdir that includes the
              -- $compiler/lib/ part of libsubdir, so we only want the
              -- $pkgid part in the package.conf file. This is a bit of
              -- a hack, really.
              progs = withPrograms lbi
              prog = ConfiguredProgram {
                         programId = programName ghcPkgProgram,
                         programVersion = Nothing,
                         programArgs = ["--force", "--global-conf", ghcpkgconf],
                         programLocation = UserSpecified ghcpkg
                     }
              progs' = updateProgram prog progs
              i_reg = i { prefixDirTemplate  = toPathTemplate ipref,
                          binDirTemplate     = toPathTemplate ibindir,
                          libDirTemplate     = toPathTemplate ilibdir,
                          libexecDirTemplate = toPathTemplate ilibexecdir,
                          dataDirTemplate    = toPathTemplate idatadir,
                          docDirTemplate     = toPathTemplate idocdir,
                          htmlDirTemplate    = toPathTemplate ihtmldir_reg
                        }
              lbi_reg = lbi { installDirTemplates = i_reg,
                              withPrograms = progs' }
          (copyHook simpleUserHooks) pd_copy lbi_copy userHooks copyFlags
          (regHook simpleUserHooks)  pd_reg  lbi_reg  userHooks registerFlags
          return ()

-- Get the build info, merging the setup-config and buildinfo files.
getConfig :: Verbosity -> IO LocalBuildInfo
getConfig verbosity = do
    lbi <- getPersistBuildConfig
    maybe_infoFile <- defaultHookedPackageDesc
    case maybe_infoFile of
        Nothing -> return lbi
        Just infoFile -> do
            hbi <- readHookedBuildInfo verbosity infoFile
            return lbi { localPkgDescr = updatePackageDescription hbi (localPkgDescr lbi)}


